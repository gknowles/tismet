// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.cpp - tismet
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
    kPageTypeFree = 'F',
    kPageTypeMaster = 'M',
    kPageTypeMetric = 'm',
    kPageTypeRadix = 'r',
    kPageTypeData = 'd',
    kPageTypeBranch = 'b',
    kPageTypeLeaf = 'l',
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
    uint32_t pages[1];
};

struct MetricPage {
    static const PageType type = kPageTypeMetric;
    PageHeader hdr;
    char name[kMaxMetricNameLen];
    uint32_t id;
    Duration interval;
    Duration retention;
    uint32_t lastPage;

    // internal radix "subpage"
    unsigned height;
    uint32_t pages[1];
};

struct DataPage {
    static const PageType type = kPageTypeData;
    PageHeader hdr;
    uint32_t id;
    TimePoint firstTime;
    uint16_t lastEntry;
    float values[1];
};

struct MetricInfo {
    Duration interval;
    uint32_t infoPage;
    uint32_t lastPage;
    TimePoint firstTime;
    uint16_t lastEntry;
};

class TsdFile {
public:
    ~TsdFile();

    bool open(string_view name);
    bool insertMetric(uint32_t & out, const string & name);
    void writeData(uint32_t id, TimePoint time, float value);

    bool findMetric(uint32_t & out, const string & name) const;

private:
    bool loadMetricInfo (uint32_t pgno, bool root);
    bool loadFreePages ();

    template<typename T> const T * addr(uint32_t pgno) const;
    template<> const PageHeader * addr<PageHeader>(uint32_t pgno) const;

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

    template<typename T> 
    void writePage(T & data, size_t count = sizeof(T)) const;
    void writePage(uint32_t pgno, const void * ptr, size_t count) const;

    bool btreeInsert(uint32_t rpn, string_view name, string_view data);
    bool radixInsert(uint32_t root, uint32_t index, uint32_t value);

    size_t valuesPerPage() const;

    unordered_map<string, uint32_t> m_metricIds;
    vector<MetricInfo> m_metricInfo;
    priority_queue<uint32_t, vector<uint32_t>, greater<uint32_t>> m_freeIds;
    RadixDigits m_rd;

    const MasterPage * m_hdr{nullptr};
    FileHandle m_data;
    FileHandle m_log;
};
} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TsdFileHandle, TsdFile> s_files;

static auto & s_perfCount = uperf("perfs (total)");
static auto & s_perfCreated = uperf("perfs created");
static auto & s_perfDeleted = uperf("perfs deleted");

static auto & s_perfOld = uperf("perf data ignored (old)");
static auto & s_perfDup = uperf("perf data duplicate");
static auto & s_perfUpdate = uperf("perf data added");


/****************************************************************************
*
*   TsdFile
*
***/

//===========================================================================
TsdFile::~TsdFile () {
    s_perfCount -= (unsigned) m_metricInfo.size();
    fileClose(m_data);
    fileClose(m_log);
}

//===========================================================================
bool TsdFile::open(string_view name) {
    m_data = fileOpen(name, File::fCreat | File::fReadWrite);
    if (!m_data)
        return false;
    if (!fileSize(m_data)) {
        MasterPage tmp = {};
        tmp.hdr.type = kPageTypeMaster;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = kDefaultPageSize;
        tmp.numPages = 1;
        fileWriteWait(m_data, 0, &tmp, sizeof(tmp));
    }
    const char * base;
    if (!fileOpenView(base, m_data)) 
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

    m_rd.init(
        m_hdr->pageSize, 
        offsetof(MetricPage, pages),
        offsetof(RadixPage, pages)
    );

    if (!loadMetricInfo(m_hdr->metricInfoRoot, true))
        return false;
    if (!loadFreePages())
        return false;

    s_perfCount += (unsigned) m_metricInfo.size();
    return true;
}

//===========================================================================
bool TsdFile::loadMetricInfo (uint32_t pgno, bool root) {
    if (!pgno)
        return true;

    auto p = addr<PageHeader>(pgno);
    if (!p)
        return false;
    auto count = root ? m_rd.rootEntries() : m_rd.pageEntries();

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < count; ++i) {
            if (!loadMetricInfo(rp->pages[i], false))
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
        mi.lastPage = mp->lastPage;
        return true;
    }

    return false;
}

//===========================================================================
bool TsdFile::loadFreePages () {
    auto pgno = m_hdr->freePageRoot;
    while (pgno) {
        auto p = addr<PageHeader>(pgno);
        if (!p || p->type != kPageTypeFree)
            return false;
        if (m_metricInfo.size() <= pgno)
            m_metricInfo.resize(pgno + 1);
        if (m_metricInfo[pgno].lastPage)
            return false;
        m_metricInfo[pgno].lastPage = pgno;
        m_freeIds.push(pgno);
        auto fp = reinterpret_cast<const FreePage*>(p);
        pgno = fp->nextPage;
    }
    return true;
}

//===========================================================================
bool TsdFile::btreeInsert(uint32_t rpn, string_view name, string_view data) {
    auto ph = addr<LeafPage>(rpn);
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
bool TsdFile::insertMetric(uint32_t & out, const string & name) {
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
    out = id;
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
    mi.infoPage = sp->hdr.pgno;
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
    s_perfCount += 1;
    return true;
}

//===========================================================================
void TsdFile::writeData(uint32_t id, TimePoint time, float value) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto count = valuesPerPage();
    if (!mi.lastPage) {
        auto mp = editPage<MetricPage>(mi.infoPage);

        auto dp = allocPage<DataPage>();
        dp->id = id;
        dp->lastEntry = (uint16_t) (id % count);
        dp->firstTime = time - dp->lastEntry * mi.interval;
        for (auto i = 0; i < count; ++i) 
            dp->values[i] = NAN;
        writePage(*dp, m_hdr->pageSize);

        mp->lastPage = dp->hdr.pgno;
        mi.lastPage = mp->lastPage;
        mi.firstTime = dp->firstTime;
        mi.lastEntry = dp->lastEntry;
        writePage(*mp);
    }
    if (mi.firstTime == TimePoint{}) {
        auto dp = addr<DataPage>(mi.lastPage);
        mi.firstTime = dp->firstTime;
        mi.lastEntry = dp->lastEntry;
    }

    // updating the last page?
    auto lastTime = mi.firstTime + mi.lastEntry * mi.interval;
    auto lastPageTime = mi.firstTime + valuesPerPage() * mi.interval;
    if (time >= mi.firstTime && time < lastPageTime) {
        auto dp = editPage<DataPage>(mi.lastPage);
        auto ent = (time - mi.firstTime) / mi.interval;
        dp->values[ent] = value;
        if (ent > mi.lastEntry) {
            for (auto i = mi.lastEntry + 1; i < ent; ++i) 
                dp->values[i] = NAN;
            mi.lastEntry = dp->lastEntry = (uint16_t) ent;
        }
        writePage(*dp, m_hdr->pageSize);
        return;
    }        

    auto t = (time - mi.firstTime) / mi.interval;
    (void) t;

    //dp->values[dp->firstEntry]
}

//===========================================================================
bool TsdFile::radixInsert(uint32_t root, uint32_t index, uint32_t value) {
    int digits[10];
    auto count = m_rd.convert(digits, size(digits), index);
    count -= 1;
    auto rp = addr<RadixPage>(root);
    while (rp->height < count) {
        auto dup = dupPage(*rp);
        writePage(*dup);

        auto nrp = editPage(*rp);
        nrp->height += 1;
        memset(nrp->pages, 0, m_hdr->pageSize - offsetof(RadixPage, pages));
        nrp->pages[0] = dup->hdr.pgno;
        writePage(*nrp);
    }
    int * d = digits;
    while (count) {
        int pos = (rp->height > count) ? 0 : *d;
        if (!rp->pages[pos]) {
            auto next = allocPage<RadixPage>();
            next->height = rp->height - 1;
            writePage(*next);
            auto nrp = dupPage(*rp);
            nrp->pages[pos] = next->hdr.pgno;
            writePage(*nrp, m_hdr->pageSize);
            assert(rp->pages[pos]);
        }
        rp = addr<RadixPage>(rp->pages[pos]);
        if (rp->height == count) {
            d += 1;
            count -= 1;
        }
    }
    if (rp->pages[*d]) 
        return false;

    auto nrp = editPage(*rp);
    nrp->pages[*d] = value;
    writePage(*nrp, m_hdr->pageSize);
    return true;
}

//===========================================================================
size_t TsdFile::valuesPerPage() const {
    return (m_hdr->pageSize - offsetof(DataPage, values)) 
        / sizeof(float);
}

//===========================================================================
uint32_t TsdFile::allocPgno () {
    auto pgno = m_hdr->freePageRoot;
    auto pageSize = m_hdr->pageSize;
    auto mp = *m_hdr;
    if (!pgno) {
        pgno = m_hdr->numPages;
        mp.numPages += 1;
        fileExtendView(m_data, (pgno + 1) * pageSize);
    } else {
        auto fp = addr<FreePage>(pgno);
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
    auto fp = *addr<FreePage>(pgno);
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
    return editPage(*addr<T>(pgno));
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
    return dupPage(*addr<T>(pgno));
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
const T * TsdFile::addr(uint32_t pgno) const {
    assert(pgno < m_hdr->numPages);
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const T*>(vptr);
    assert(ptr->hdr.type == ptr->type);
    return ptr;
}

//===========================================================================
template<>
const PageHeader * TsdFile::addr<PageHeader>(uint32_t pgno) const {
    if (pgno > m_hdr->numPages)
        return nullptr;
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const PageHeader*>(vptr);
    return ptr;
}

//===========================================================================
template<typename T>
void TsdFile::writePage(T & data, size_t count) const {
    writePage(data.hdr.pgno, &data, count);
}

//===========================================================================
void TsdFile::writePage(uint32_t pgno, const void * ptr, size_t count) const {
    assert(pgno < m_hdr->numPages);
    assert(count <= m_hdr->pageSize);
    fileWriteWait(m_data, pgno * m_hdr->pageSize, ptr, count);
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
TsdFileHandle tsdOpen(string_view name) {
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
bool tsdFindMetric(uint32_t & out, TsdFileHandle h, string_view name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->findMetric(out, string(name));
}

//===========================================================================
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, string_view name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->insertMetric(out, string(name));
}

//===========================================================================
void tsdWriteData(TsdFileHandle h, uint32_t id, TimePoint time, float value) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->writeData(id, time, value);
}
