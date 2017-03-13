// tsdata.cpp - tismet tsdata
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

const unsigned kMaxMetricNameLen = 128;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());

const unsigned kDefaultPageSize = 256;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize));

const unsigned kDataFileSig[] = { 
    0x39515728, 
    0x4873456d, 
    0xf6bfd8a1, 
    0xa33f3ba2 
};

namespace {
enum PageType {
    kPageTypeFree,
    kPageTypeMaster,
    kPageTypeBranch,
    kPageTypeLeaf,
    kPageTypeMetric,
    kPageTypeData,
    kPageTypeRadix,
};

struct PageHeader {
    unsigned type;
    uint32_t pgno;
    uint32_t checksum;
    uint64_t lsn;
};

struct MasterPage {
    static const PageType type = kPageTypeMaster;
    PageHeader hdr;
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned numPages;
    unsigned freePageRoot;
    unsigned metricInfoRoot;
};
static_assert(is_standard_layout<MasterPage>::value);

struct FreePage {
    static const PageType type = kPageTypeFree;
    PageHeader hdr;
    unsigned nextPage;
};

struct LeafPage {
    static const PageType type = kPageTypeLeaf;
    PageHeader hdr;
    char entries[1];
};

struct RadixPage {
    static const PageType type = kPageTypeRadix;
    PageHeader hdr;
    unsigned height;
    uint32_t values[1];
};

struct MetricPage {
    static const PageType type = kPageTypeMetric;
    PageHeader hdr;
    char name[kMaxMetricNameLen];
    uint32_t id;
    Duration interval;
    Duration retention;
    uint32_t firstPage;
};

struct MetricInfo {
    Duration interval;
    uint32_t infoPage;
    TimePoint firstTime;
    uint32_t firstPage;
    uint32_t firstEntry;
};

class TsdFile {
public:
    bool open(const string & name);
    bool insertMetric(uint32_t & out, const string & name);
    void insertData(uint32_t id, TimePoint time, float data);

    bool findMetric(uint32_t & out, const string & name) const;

private:
    bool loadMetricInfo (uint32_t pgno);
    bool loadFreePages ();

    template<typename T> const T * pageAddr(uint32_t pgno) const;
    template<> const PageHeader * pageAddr<PageHeader>(uint32_t pgno) const;

    uint32_t allocPgno();
    template<typename T> unique_ptr<T> allocPage();
    void freePage(uint32_t pgno);
    template<typename T> unique_ptr<T> allocPage(uint32_t pgno) const;

    // get copy of page to update and then write
    template<typename T> unique_ptr<T> editPage(uint32_t pgno) const;
    template<typename T> unique_ptr<T> editPage(const T & data) const;

    // allocate new page (with new id) that is a copy of an existing page
    template<typename T> unique_ptr<T> dupPage(uint32_t pgno);
    template<typename T> unique_ptr<T> dupPage(const T & data);

    template<typename T> void writePage(T & data) const;
    void writePage(uint32_t pgno, const void * ptr, size_t count) const;

    bool btreeInsert(
        uint32_t rpn,
        const std::string & name,
        const std::string & data
    );
    bool radixInsert(uint32_t root, uint32_t index, uint32_t value);

    unordered_map<string, uint32_t> m_metricIds;
    vector<MetricInfo> m_metricInfo;
    priority_queue<uint32_t, vector<uint32_t>, greater<uint32_t>> m_freeIds;
    RadixDigits m_rd;

    const MasterPage * m_hdr{nullptr};
    unique_ptr<IFile> m_data;
    unique_ptr<IFile> m_log;
};
} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TsdFileHandle, TsdFile> s_files;


/****************************************************************************
*
*   TsdFile
*
***/

//===========================================================================
bool TsdFile::open(const string & name) {
    m_data = fileOpen(name, IFile::kCreat | IFile::kReadWrite);
    auto file = m_data.get();
    if (!file)
        return false;
    if (!fileSize(file)) {
        MasterPage tmp = {};
        tmp.hdr.type = kPageTypeMaster;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = kDefaultPageSize;
        tmp.numPages = 1;
        fileWriteSync(m_data.get(), 0, &tmp, sizeof(tmp));
    }
    const char * base;
    if (!fileOpenView(base, file)) 
        return false;
    m_hdr = (const MasterPage *)base;
    if (memcmp(
        m_hdr->signature, 
        kDataFileSig, 
        sizeof(m_hdr->signature)
    ) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }

    m_rd.init(m_hdr->pageSize - offsetof(RadixPage, values));

    if (!loadMetricInfo(m_hdr->metricInfoRoot))
        return false;
    if (!loadFreePages())
        return false;

    return true;
}

//===========================================================================
bool TsdFile::loadMetricInfo (uint32_t pgno) {
    if (!pgno)
        return true;

    auto p = pageAddr<PageHeader>(pgno);
    if (!p)
        return false;
    auto count = m_rd.pageEntries();

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < count; ++i) {
            if (!loadMetricInfo(rp->values[i]))
                return false;
        }
        return true;
    }

    if (p->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<const MetricPage*>(p);
        m_metricIds[mp->name] = mp->id;
        if (m_metricInfo.size() <= mp->id)
            m_metricInfo.resize(mp->id + 1);
        auto & mi = m_metricInfo[mp->id];
        mi.infoPage = mp->hdr.pgno;
        mi.interval = mp->interval;
        mi.firstPage = mp->firstPage;
        return true;
    }

    return false;
}

//===========================================================================
bool TsdFile::loadFreePages () {
    auto pgno = m_hdr->freePageRoot;
    while (pgno) {
        auto p = pageAddr<PageHeader>(pgno);
        if (!p || p->type != kPageTypeFree)
            return false;
        if (m_metricInfo.size() <= pgno)
            m_metricInfo.resize(pgno + 1);
        if (m_metricInfo[pgno].firstPage)
            return false;
        m_metricInfo[pgno].firstPage = pgno;
        m_freeIds.push(pgno);
        auto fp = reinterpret_cast<const FreePage*>(p);
        pgno = fp->nextPage;
    }
    return true;
}

//===========================================================================
bool TsdFile::btreeInsert(
    uint32_t rpn,
    const std::string & name,
    const std::string & data
) {
    auto ph = pageAddr<LeafPage>(rpn);
    if (!ph->entries[0]) {
        auto lp = (LeafPage *) alloca(
            sizeof(LeafPage) + name.size() + data.size() + 1);  
        lp->hdr.type = kPageTypeLeaf;
        char * lpe = lp->entries;
        *lpe++ = (char) name.size();
        memcpy(lpe, name.data(), name.size());
        lpe += name.size();
        *lpe++ = (char) data.size();
        memcpy(lpe, data.data(), data.size());
        lpe += data.size();
        *lpe++ = 0;
        writePage(rpn, lp, lpe - (char *) lp);
        return true;
    }
    while (ph->hdr.type == kPageTypeBranch) {
    }
    return false;
}

//===========================================================================
bool TsdFile::findMetric(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end())
        return false;
    out = i->second;
    return true;
}

//===========================================================================
bool TsdFile::insertMetric(uint32_t & out, const std::string & name) {
    assert(!name.empty());
    assert(name.size() < kMaxMetricNameLen);
    auto i = m_metricIds.find(name);
    if (i != m_metricIds.end()) {
        out = i->second;
        return false;
    }

    // get metric id
    uint32_t id;
    if (m_freeIds.empty()) {
        id = (uint32_t) m_metricInfo.size();
        m_metricInfo.push_back({});
    } else {
        id = m_freeIds.top();
        m_freeIds.pop();
    }
    m_metricIds[name] = id;

    // set info page 
    auto sp = allocPage<MetricPage>();
    auto count = name.copy(sp->name, size(sp->name) - 1);
    sp->name[count] = 0;
    sp->id = id;
    sp->interval = 1min;
    sp->retention = 30min;
    writePage(*sp);

    auto & mi = m_metricInfo[id];
    mi = {};
    mi.infoPage = id;
    mi.interval = sp->interval;

    // update index
    if (!m_hdr->metricInfoRoot) {
        auto rp = allocPage<RadixPage>();
        rp->height = 0;
        writePage(*rp);
        auto mp = *m_hdr;
        mp.metricInfoRoot = rp->hdr.pgno;
        writePage(mp);
    }
    bool inserted = radixInsert(m_hdr->metricInfoRoot, id, sp->hdr.pgno);
    assert(inserted);
    return true;
}

//===========================================================================
bool TsdFile::radixInsert(uint32_t root, uint32_t index, uint32_t value) {
    int digits[10];
    auto count = m_rd.convert(digits, size(digits), index);
    count -= 1;
    auto rp = pageAddr<RadixPage>(root);
    while (rp->height < count) {
        auto dup = dupPage(*rp);
        writePage(*dup);

        auto nrp = editPage(*rp);
        nrp->height += 1;
        memset(nrp->values, 0, m_hdr->pageSize - offsetof(RadixPage, values));
        nrp->values[0] = dup->hdr.pgno;
        writePage(*nrp);
    }
    int * d = digits;
    while (count) {
        int pos = (rp->height > count) ? 0 : *d;
        if (!rp->values[pos]) {
            auto next = allocPage<RadixPage>();
            next->height = rp->height - 1;
            writePage(*next);
            auto nrp = dupPage(*rp);
            nrp->values[pos] = next->hdr.pgno;
            writePage(*nrp);
            assert(rp->values[pos]);
        }
        rp = pageAddr<RadixPage>(rp->values[pos]);
        if (rp->height == count) {
            d += 1;
            count -= 1;
        }
    }
    if (rp->values[*d]) 
        return false;

    auto nrp = editPage(*rp);
    nrp->values[*d] = value;
    writePage(*nrp);
    return true;
}

//===========================================================================
uint32_t TsdFile::allocPgno () {
    auto file = m_data.get();
    auto pgno = m_hdr->freePageRoot;
    auto pageSize = m_hdr->pageSize;
    auto mp = *m_hdr;
    if (!pgno) {
        pgno = m_hdr->numPages;
        mp.numPages += 1;
        fileExtendView(file, (pgno + 1) * pageSize);
    } else {
        auto fp = pageAddr<FreePage>(pgno);
        assert(fp->hdr.type == kPageTypeFree);
        mp.freePageRoot = fp->nextPage;
    }
    writePage(mp);
    return pgno;
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::allocPage() {
    auto pgno = allocPgno();
    return allocPage<T>(pgno);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::allocPage(uint32_t pgno) const {
    auto pageSize = m_hdr->pageSize;
    void * vptr = new char[pageSize];
    memset(vptr, 0, pageSize);
    T * ptr = new(vptr) T;
    ptr->hdr.type = ptr->type;
    ptr->hdr.pgno = pgno;
    ptr->hdr.checksum = 0;
    ptr->hdr.lsn = 0;
    return unique_ptr<T>(ptr);
}

//===========================================================================
void TsdFile::freePage(uint32_t pgno) {
    assert(pgno < m_hdr->numPages);
    auto fp = *pageAddr<FreePage>(pgno);
    assert(fp.hdr.type != kPageTypeFree);
    fp.hdr.type = kPageTypeFree;
    fp.nextPage = m_hdr->freePageRoot;
    writePage(fp);
    auto mp = *m_hdr;
    mp.freePageRoot = pgno;
    writePage(mp);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::editPage(uint32_t pgno) const {
    return editPage(*pageAddr<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::editPage(const T & data) const {
    auto pageSize = m_hdr->pageSize;
    void * vptr = new char[pageSize];
    T * ptr = new(vptr) T;
    memcpy(ptr, &data, pageSize);
    return unique_ptr<T>(ptr);
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::dupPage(uint32_t pgno) {
    return dupPage(*pageAddr<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> TsdFile::dupPage(const T & data) {
    auto ptr = editPage(data);
    ptr->hdr.pgno = allocPgno();
    return ptr;
}

//===========================================================================
template<typename T>
const T * TsdFile::pageAddr(uint32_t pgno) const {
    assert(pgno < m_hdr->numPages);
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const T*>(vptr);
    assert(ptr->hdr.type == ptr->type);
    return ptr;
}

//===========================================================================
template<>
const PageHeader * TsdFile::pageAddr<PageHeader>(uint32_t pgno) const {
    if (pgno > m_hdr->numPages)
        return nullptr;
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const PageHeader*>(vptr);
    return ptr;
}

//===========================================================================
template<typename T>
void TsdFile::writePage(T & data) const {
    writePage(data.hdr.pgno, &data, sizeof(data));
}

//===========================================================================
void TsdFile::writePage(uint32_t pgno, const void * ptr, size_t count) const {
    assert(pgno < m_hdr->numPages);
    assert(count <= m_hdr->pageSize);
    fileWriteSync(m_data.get(), pgno * m_hdr->pageSize, ptr, count);
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
TsdFileHandle tsdOpen(const string & name) {
    auto tsd = make_unique<TsdFile>();
    if (!tsd->open(name))
        return TsdFileHandle{};

    auto h = s_files.insert(tsd.release());
    return h;
}

//===========================================================================
void tsdClose(TsdFileHandle h) {
    s_files.erase(h);
}

//===========================================================================
bool tsdFindMetric(uint32_t & out, TsdFileHandle h, const std::string & name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->findMetric(out, name);
}

//===========================================================================
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, const std::string & name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->insertMetric(out, name);
}
