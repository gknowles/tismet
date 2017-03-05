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
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max(), "");

const unsigned kDefaultPageSize = 256;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize), "");

const unsigned kDataFileSig[] = { 
    0x39515728, 
    0x4873456d, 
    0xf6bfd8a1, 
    0xa33f3ba2 
};

namespace {
enum PageType {
    PAGE_TYPE_FREE,
    PAGE_TYPE_MASTER,
    PAGE_TYPE_BRANCH,
    PAGE_TYPE_LEAF,
    PAGE_TYPE_METRIC,
    PAGE_TYPE_DATA,
    PAGE_TYPE_RADIX,
};

struct PageHeader {
    unsigned type;
    uint32_t checksum;
    uint64_t lsn;
};

struct MasterPage {
    PageHeader hdr;
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned numPages;
    unsigned freePageRoot;
    unsigned metricInfoRoot;
};

struct FreePage {
    PageHeader hdr;
    unsigned nextPage;
};

struct LeafPage {
    PageHeader hdr;
    char entries[1];
};

struct RadixPage {
    PageHeader hdr;
    unsigned prefixSize;
    unsigned prefix;
    uint32_t values[1];
};

struct MetricPage {
    PageHeader hdr;
    char name[kMaxMetricNameLen];
    Duration m_interval;
    Duration m_retention;
    uint32_t m_firstPage;
};

struct MetricInfo {
    Duration m_interval;
    uint32_t m_infoPage;
    TimePoint m_firstTime;
    uint32_t m_firstPage;
    uint32_t m_firstEntry;
};

class TsdFile {
public:
    bool open(const string & name);
    bool insertMetric(uint32_t & out, const std::string & name);
    void insertData(uint32_t id, TimePoint time, float data);

private:
    uint32_t allocPage();
    void freePage(uint32_t pgno);

    template<typename T>
    const T * pageAddr(uint32_t pgno) const;

    template<typename T>
    void writePage(uint32_t pgno, T & data) const;
    void writePage(uint32_t pgno, const void * ptr, size_t count) const;

    bool btreeInsert(
        uint32_t rpn,
        const std::string & name,
        const std::string & data
    );

    unordered_map<string, uint32_t> m_metricIds;
    vector<MetricInfo> m_metricInfo;
    priority_queue<uint32_t, vector<uint32_t>, greater<uint32_t>> m_freeIds;

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
        tmp.hdr.type = PAGE_TYPE_MASTER;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = kDefaultPageSize;
        tmp.numPages = 1;
        writePage(0, tmp);
    }
    const char * base;
    if (!fileOpenView(base, file)) 
        return false;
    m_hdr = (const MasterPage *)base;
    if (memcmp(m_hdr->signature, kDataFileSig, sizeof(m_hdr->signature)) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }
    return true;
}

//===========================================================================
bool TsdFile::btreeInsert(
    uint32_t rpn,
    const std::string & name,
    const std::string & data
) {
    auto ph = pageAddr<PageHeader>(rpn);
    if (ph->type == PAGE_TYPE_FREE) {
        auto lp = (LeafPage *) alloca(
            sizeof(LeafPage) + name.size() + data.size() + 1);  
        lp->hdr.type = PAGE_TYPE_LEAF;
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
    while (ph->type == PAGE_TYPE_BRANCH) {
    }
    return false;
}

//===========================================================================
bool TsdFile::insertMetric(uint32_t & out, const std::string & name) {
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
    auto ib = m_metricIds.insert(make_pair(name, id));
    assert(ib.second);

    // set info page 
    //auto pn = allocPage();
//    auto mp = pageAddr<MetricPage>(pn);
    MetricPage mp;
    mp.hdr.type = PAGE_TYPE_METRIC;



    //bool inserted = radixInsert(id, pn);
    //assert(inserted);

    return false;
}

//===========================================================================
uint32_t TsdFile::allocPage() {
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
        assert(fp->hdr.type == PAGE_TYPE_FREE);
        mp.freePageRoot = fp->nextPage;
    }

    writePage(0, mp);
    return pgno;
}

//===========================================================================
void TsdFile::freePage(uint32_t pgno) {
    assert(pgno < m_hdr->numPages);
    auto fp = *pageAddr<FreePage>(pgno);
    assert(fp.hdr.type != PAGE_TYPE_FREE);
    fp.hdr.type = PAGE_TYPE_FREE;
    fp.nextPage = m_hdr->freePageRoot;
    writePage(pgno, fp);
    auto mp = *m_hdr;
    mp.freePageRoot = pgno;
    writePage(0, mp);
}

//===========================================================================
template<typename T>
const T * TsdFile::pageAddr(uint32_t pgno) const {
    assert(pgno < m_hdr->numPages);
    const void * ptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    return static_cast<const T*>(ptr);
}

//===========================================================================
template<typename T>
void TsdFile::writePage(uint32_t pgno, T & data) const {
    writePage(pgno, &data, sizeof(data));
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
    return false;
}

//===========================================================================
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, const std::string & name) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->insertMetric(out, name);
}
