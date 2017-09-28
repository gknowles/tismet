// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.cpp - tismet data
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

const char kDumpVersion[] = "Tismet Dump Version 2017.1";

const unsigned kMaxMetricNameLen = 64;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());

const unsigned kDefaultPageSize = 4096;
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

    // EXTENDS BEYOND END OF STRUCT
    char entries[1];
};

struct RadixData {
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    uint32_t pages[1];
};

struct RadixPage {
    static const PageType type = kPageTypeRadix;
    PageHeader hdr;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd; 
};

struct MetricPage {
    static const PageType type = kPageTypeMetric;
    PageHeader hdr;
    char name[kMaxMetricNameLen];
    uint32_t id;
    Duration interval;
    Duration retention;
    uint32_t lastPage;
    unsigned lastPagePos;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd; 
};

struct DataPage {
    static const PageType type = kPageTypeData;
    PageHeader hdr;
    uint32_t id;

    // time of first value on page
    TimePoint firstPageTime; 
    
    // Position of last value, values that come after this on the page are 
    // either in the not yet populated future or (because it's a giant 
    // discontinuous ring buffer) in the distant past.
    uint16_t lastPageValue; 

    // EXTENDS BEYOND END OF STRUCT
    float values[1];
};

struct MetricInfo {
    Duration interval;
    uint32_t infoPage;
    uint32_t lastPage; // page with most recent data values
    TimePoint firstPageTime; // time of first value on last page
    uint16_t lastPageValue; // position of last value on last page
};

class TsdFile : public HandleContent {
public:
    ~TsdFile();

    bool open(string_view name, size_t pageSize);
    bool insertMetric(uint32_t & out, const string & name);
    void eraseMetric(uint32_t id);
    void updateMetric(uint32_t id, Duration retention, Duration interval);
    
    void updateValue(uint32_t id, TimePoint time, float value);

    bool findMetric(uint32_t & out, const string & name) const;
    void findMetrics(UnsignedSet & out, string_view name) const;

    size_t enumValues(
        ITsdEnumNotify * notify, 
        uint32_t id, 
        TimePoint first, 
        TimePoint last
    );

private:
    bool loadMetricInfo(uint32_t pgno);
    void metricFreePage(uint32_t pgno);

    bool loadFreePages ();
    uint32_t allocPgno();
    template<typename T> unique_ptr<T> allocPage();
    void freePage(uint32_t pgno);
    template<typename T> unique_ptr<T> allocPage(uint32_t pgno) const;

    template<typename T> const T * viewPage(uint32_t pgno) const;
    template<> const PageHeader * viewPage<PageHeader>(uint32_t pgno) const;

    // get copy of page to update and then write
    template<typename T> unique_ptr<T> editPage(uint32_t pgno) const;
    template<typename T> unique_ptr<T> editPage(const T & data) const;

    // allocate new page (with new id) that is a copy of an existing page
    template<typename T> unique_ptr<T> dupPage(uint32_t pgno);
    template<typename T> unique_ptr<T> dupPage(const T & data);

    template<typename T> 
    void writePage(T & data, size_t count = sizeof(T)) const;
    template<> 
    void writePage<PageHeader>(PageHeader & hdr, size_t count) const;
    void writePage(uint32_t pgno, const void * ptr, size_t count) const;

    void radixClear(PageHeader & hdr);
    void radixErase(PageHeader & hdr, size_t firstPos, size_t lastPos);
    void radixFreePage(uint32_t pgno);
    bool radixFind(uint32_t * out, uint32_t root, size_t pos);
    bool radixInsert(uint32_t root, size_t pos, uint32_t value);
    bool radixFind(
        PageHeader const ** hdr, 
        RadixData const ** rd, 
        size_t * rpos,
        uint32_t root, 
        size_t pos
    );

    unique_ptr<DataPage> allocDataPage(uint32_t id, TimePoint time);
    size_t valuesPerPage() const;
    bool findDataPage(
        uint32_t * dataPgno, 
        unsigned * pagePos, 
        uint32_t id,
        TimePoint time
    );

    void indexInsertMetric(uint32_t id, const string & name);
    void indexEraseMetric(uint32_t id, const string & name);

    vector<MetricInfo> m_metricInfo;
    unordered_map<string, uint32_t> m_metricIds;
    UnsignedSet m_ids;
    
    struct UnsignedSetWithCount {
        UnsignedSet uset;
        size_t count{0};
    };

    // metric ids by name length as measured in segments
    vector<UnsignedSetWithCount> m_lenIds;

    // Index of metric ids by value of segments of their names. So the 
    // wildcard *.red.* could be matched by finding all the metrics whose name
    // has "red" as the second segment (m_segIds[1]["red"]) and three segments
    // long (m_lenIds[3]).
    vector<unordered_map<string, UnsignedSetWithCount>> m_segIds;

    RadixDigits m_rdIndex;
    RadixDigits m_rdMetric;

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

static auto & s_perfCount = uperf("metrics (total)");
static auto & s_perfCreated = uperf("metrics created");
static auto & s_perfDeleted = uperf("metrics deleted");

static auto & s_perfOld = uperf("metric values ignored (old)");
static auto & s_perfDup = uperf("metric values duplicate");
static auto & s_perfAdd = uperf("metric values added");


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
bool TsdFile::open(string_view name, size_t pageSize) {
    assert(pageSize == pow2Ceil(pageSize));
    if (!pageSize)
        pageSize = kDefaultPageSize;

    m_data = fileOpen(name, File::fCreat | File::fReadWrite);
    if (!m_data)
        return false;
    if (!fileSize(m_data)) {
        MasterPage tmp = {};
        tmp.hdr.type = kPageTypeMaster;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = (unsigned) pageSize;
        tmp.numPages = 1;
        fileWriteWait(m_data, 0, &tmp, sizeof(tmp));
    }
    const char * base;
    if (!fileOpenView(
        base, 
        m_data, 
        numeric_limits<uint32_t>::max() * filePageSize()
    )) {
        return false;
    }
    m_hdr = (const MasterPage *)base;
    if (memcmp(
        m_hdr->signature, 
        kDataFileSig, 
        sizeof(m_hdr->signature)
    ) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }

    auto ipOff = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    m_rdIndex.init(m_hdr->pageSize, ipOff, ipOff);
    auto mpOff = offsetof(MetricPage, rd) + offsetof(RadixData, pages);
    m_rdMetric.init(m_hdr->pageSize, mpOff, ipOff);

    if (!loadMetricInfo(m_hdr->metricInfoRoot))
        return false;
    if (!loadFreePages())
        return false;

    s_perfCount += (unsigned) m_metricInfo.size();
    return true;
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void TsdFile::metricFreePage (uint32_t pgno) {
    auto mp = viewPage<MetricPage>(pgno);
    for (int i = 0; i < mp->rd.numPages; ++i) {
        if (auto pn = mp->rd.pages[i])
            freePage(pn);
    }
    m_metricInfo[mp->id] = {};
    indexEraseMetric(mp->id, mp->name);
}

//===========================================================================
bool TsdFile::loadMetricInfo (uint32_t pgno) {
    if (!pgno)
        return true;

    auto p = viewPage<PageHeader>(pgno);
    if (!p)
        return false;

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < rp->rd.numPages; ++i) {
            if (!loadMetricInfo(rp->rd.pages[i]))
                return false;
        }
        return true;
    }

    if (p->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<const MetricPage*>(p);
        
        indexInsertMetric(mp->id, mp->name);

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
bool TsdFile::findMetric(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end())
        return false;
    out = i->second;
    return true;
}

//===========================================================================
void TsdFile::findMetrics(UnsignedSet & out, string_view name) const {
    if (name.empty()) {
        out = m_ids;
        return;
    }

    QueryInfo qry;
    [[maybe_unused]] bool result = queryParse(qry, name);
    assert(result);
    if (~qry.flags & QueryInfo::fWild) {
        uint32_t id;
        out.clear();
        if (findMetric(id, string(name)))
            out.insert(id);
        return;
    }

    vector<QueryInfo::PathSegment> segs;
    queryPathSegments(segs, qry);
    auto numSegs = segs.size();
    vector<const UnsignedSetWithCount*> usets(numSegs);
    auto fewest = &m_lenIds[numSegs];
    int ifewest = -1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto & seg = segs[i];
        if (~seg.flags & QueryInfo::fWild) {
            auto it = m_segIds[i].find(string(seg.prefix));
            if (it != m_segIds[i].end()) {
                usets[i] = &it->second;
                if (it->second.count < fewest->count) {
                    ifewest = i;
                    fewest = &it->second;
                }
            }
        }
    }
    out = fewest->uset;
    for (int i = 0; i < numSegs; ++i) {
        if (i == ifewest)
            continue;
        if (auto usetw = usets[i]) {
            out.intersect(usetw->uset);
            continue;
        }
        auto & seg = segs[i];
        UnsignedSet found;
        for (auto && kv : m_segIds[i]) {
            if (queryMatchSegment(seg.node, kv.first)) {
                if (found.empty()) {
                    found = kv.second.uset;
                } else {
                    found.insert(kv.second.uset);
                }
            }
        }
        out.intersect(move(found));
    }
}

//===========================================================================
void TsdFile::indexInsertMetric(uint32_t id, const string & name) {
    m_metricIds[name] = id;
    m_ids.insert(id);
    vector<string_view> segs;
    strSplit(segs, name, '.');
    auto numSegs = segs.size();
    if (m_lenIds.size() <= numSegs) {
        m_lenIds.resize(numSegs + 1);
        m_segIds.resize(numSegs);
    }
    m_lenIds[numSegs].uset.insert(id);
    m_lenIds[numSegs].count += 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto & ids = m_segIds[i][string(segs[i])];
        ids.uset.insert(id);
        ids.count += 1;
    }
}

//===========================================================================
void TsdFile::indexEraseMetric(uint32_t id, const string & name) {
    auto num = m_metricIds.erase(name);
    assert(num == 1);
    m_ids.erase(id);
    vector<string_view> segs;
    strSplit(segs, name, '.');
    auto numSegs = segs.size();
    m_lenIds[numSegs].uset.erase(id);
    m_lenIds[numSegs].count -= 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto key = string(segs[i]);
        auto & ids = m_segIds[i][key];
        ids.uset.erase(id);
        if (--ids.count == 0)
            m_segIds[i].erase(key);
    }
    numSegs = m_segIds.size();
    for (; numSegs; --numSegs) {
        if (!m_segIds[numSegs - 1].empty())
            break;
        assert(m_lenIds[numSegs].uset.empty());
        m_lenIds.resize(numSegs);
        m_segIds.resize(numSegs - 1);
    }
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
    if (m_ids.empty()) {
        id = 1;
    } else {
        auto ids = *m_ids.ranges().begin();
        id = ids.first > 1 ? 1 : ids.second + 1;
    }
    out = id;

    // update indexes
    indexInsertMetric(id, name);

    // set info page 
    auto mp = allocPage<MetricPage>();
    auto count = name.copy(mp->name, size(mp->name) - 1);
    mp->name[count] = 0;
    mp->id = id;
    mp->interval = 1min;
    mp->retention = 30min;
    mp->rd.height = 0;
    mp->rd.numPages = (uint16_t) m_rdMetric.rootEntries();
    writePage(*mp);

    if (id >= m_metricInfo.size())
        m_metricInfo.resize(id + 1);
    auto & mi = m_metricInfo[id];
    assert(!mi.infoPage);
    mi = {};
    mi.infoPage = mp->hdr.pgno;
    mi.interval = mp->interval;

    // update index
    if (!m_hdr->metricInfoRoot) {
        auto rp = allocPage<RadixPage>();
        rp->rd.height = 0;
        rp->rd.numPages = (uint16_t) m_rdIndex.rootEntries();
        writePage(*rp);
        auto masp = *m_hdr;
        masp.metricInfoRoot = rp->hdr.pgno;
        writePage(masp);
    }
    bool inserted = radixInsert(m_hdr->metricInfoRoot, id, mp->hdr.pgno);
    assert(inserted);
    s_perfCount += 1;
    return true;
}

//===========================================================================
void TsdFile::eraseMetric(uint32_t id) {
    if (uint32_t pgno = m_metricInfo[id].infoPage) 
        metricFreePage(pgno);
}

//===========================================================================
void TsdFile::updateMetric(
    uint32_t id, 
    Duration retention, 
    Duration interval
) {
    auto & mi = m_metricInfo[id];
    auto mp = viewPage<MetricPage>(mi.infoPage);
    if (mp->retention == retention && mp->interval == interval)
        return;

    auto nmp = editPage(*mp);
    radixClear(nmp->hdr);
    nmp->lastPage = 0;
    nmp->lastPagePos = 0;
    writePage(*nmp, m_hdr->pageSize);
    mi.lastPage = 0;
    mi.firstPageTime = {};
    mi.lastPageValue = 0;
}


/****************************************************************************
*
*   Metric data values
*
***/

//===========================================================================
size_t TsdFile::valuesPerPage() const {
    return (m_hdr->pageSize - offsetof(DataPage, values)) 
        / sizeof(float);
}

//===========================================================================
unique_ptr<DataPage> TsdFile::allocDataPage(uint32_t id, TimePoint time) {
    auto vpp = valuesPerPage();
    auto dp = allocPage<DataPage>();
    dp->id = id;
    dp->lastPageValue = 0;
    dp->firstPageTime = time;
    for (auto i = 0; i < vpp; ++i) 
        dp->values[i] = NAN;
    return dp;
}

//===========================================================================
void TsdFile::updateValue(uint32_t id, TimePoint time, float value) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto vpp = valuesPerPage();

    // ensure all info about the last page is loaded, the hope is that almost
    // all updates are to the last page.
    if (!mi.lastPage) {
        auto dp = allocDataPage(id, time);
        dp->lastPageValue = (uint16_t) (id % vpp);
        dp->firstPageTime = time - dp->lastPageValue * mi.interval;
        writePage(*dp, m_hdr->pageSize);

        auto mp = editPage<MetricPage>(mi.infoPage);
        mp->lastPage = dp->hdr.pgno;
        assert(mp->lastPagePos == 0);
        mp->rd.pages[0] = mp->lastPage;
        writePage(*mp, m_hdr->pageSize);

        mi.lastPage = mp->lastPage;
        mi.firstPageTime = dp->firstPageTime;
        mi.lastPageValue = dp->lastPageValue;
    }
    if (mi.firstPageTime == TimePoint{}) {
        auto dp = viewPage<DataPage>(mi.lastPage);
        mi.firstPageTime = dp->firstPageTime;
        mi.lastPageValue = dp->lastPageValue;
    }

    auto pageInterval = vpp * mi.interval;
    auto lastValueTime = mi.firstPageTime + mi.lastPageValue * mi.interval;

    // one interval past last time on page (aka first time on next page)
    auto endPageTime = mi.firstPageTime + pageInterval; 

    // updating historical value?
    if (time <= lastValueTime) {
        auto dpno = mi.lastPage;
        if (time < mi.firstPageTime) {
            auto mp = viewPage<MetricPage>(mi.infoPage);
            if (time <= lastValueTime - mp->retention) {
                // before first value
                s_perfOld += 1;
                return;
            }
            auto off = (mi.firstPageTime - time - mi.interval) 
                / pageInterval + 1;
            auto dpages = (mp->retention + pageInterval - mi.interval) 
                / pageInterval;
            auto pagePos = 
                (uint32_t) (mp->lastPagePos + dpages - off) % dpages;
            if (!radixFind(&dpno, mi.infoPage, pagePos)) {
                auto pageTime = mi.firstPageTime - off * pageInterval;
                auto dp = allocDataPage(id, pageTime);
                dp->lastPageValue = (uint16_t) vpp - 1;
                writePage(*dp, m_hdr->pageSize);
                dpno = dp->hdr.pgno;
                bool inserted = radixInsert(mi.infoPage, pagePos, dpno);
                assert(inserted);
            }
        }
        auto dp = editPage<DataPage>(dpno);
        assert(time >= dp->firstPageTime);
        auto ent = (time - dp->firstPageTime) / mi.interval;
        assert(ent < (unsigned) vpp);
        auto & ref = dp->values[ent];
        if (isnan(ref)) {
            s_perfAdd += 1;
        } else {
            s_perfDup += 1;
        }
        ref = value;
        writePage(*dp, m_hdr->pageSize);
        return;
    }
    
    //-----------------------------------------------------------------------
    // after last known value

    // If past the end of the page, check if it's past the end of all pages
    if (time >= endPageTime) {
        auto mp = viewPage<MetricPage>(mi.infoPage);
        // further in the future than the retention period? remove all values
        // and add as new initial value.
        if (time >= lastValueTime + mp->retention) {
            auto nmp = editPage<MetricPage>(mi.infoPage);
            radixClear(nmp->hdr);
            nmp->lastPage = 0;
            nmp->lastPagePos = 0;
            writePage(*nmp, m_hdr->pageSize);
            mi.lastPage = 0;
            mi.firstPageTime = {};
            mi.lastPageValue = 0;
            updateValue(id, time, value);
            return;
        }
    }

    // update last page
    auto dp = editPage<DataPage>(mi.lastPage);
    assert(mi.firstPageTime == dp->firstPageTime);
    assert(mi.lastPageValue == dp->lastPageValue);
    auto i = mi.lastPageValue;
    for (;;) {
        i += 1;
        lastValueTime += mi.interval;
        if (lastValueTime == endPageTime)
            break;
        if (lastValueTime == time) {
            s_perfAdd += 1;
            dp->values[i] = value;
            mi.lastPageValue = dp->lastPageValue = i;
            writePage(*dp, m_hdr->pageSize);
            return;
        }
        dp->values[i] = NAN;
    }
    mi.lastPageValue = dp->lastPageValue = i;
    writePage(*dp, m_hdr->pageSize);

    //-----------------------------------------------------------------------
    // value is after last page

    // delete pages between last page and the one the value is on
    auto num = (time - endPageTime) / pageInterval;
    auto mp = editPage<MetricPage>(mi.infoPage);
    auto numValues = mp->retention / mp->interval;
    auto numPages = (numValues - 1) / vpp + 1;
    auto first = (mp->lastPagePos + 1) % numPages;
    auto last = first + num;
    if (num) {
        if (last <= numPages) {
            radixErase(mp->hdr, first, last);
        } else {
            radixErase(mp->hdr, first, numPages);
            radixErase(mp->hdr, 0, last % numPages);
        }
    }

    // update last page references
    mp->lastPagePos = (unsigned) last;
    radixFind(&mp->lastPage, mi.infoPage, last);
    if (!mp->lastPage) {
        dp = allocDataPage(id, endPageTime);
        mp->lastPage = dp->hdr.pgno;
        writePage(*mp, m_hdr->pageSize);
        bool inserted = radixInsert(mi.infoPage, mp->lastPagePos, mp->lastPage);
        assert(inserted);
        writePage(*dp, m_hdr->pageSize);
    } else {
        writePage(*mp, m_hdr->pageSize);
        dp = editPage<DataPage>(mp->lastPage);
        dp->firstPageTime = endPageTime;
        dp->lastPageValue = 0;
        writePage(*dp);
    }

    mi.lastPage = mp->lastPage;
    mi.firstPageTime = dp->firstPageTime;
    mi.lastPageValue = dp->lastPageValue;

    // write value to new last page
    updateValue(id, time, value);
}

//===========================================================================
// false if time is outside of retention period
bool TsdFile::findDataPage(
    uint32_t * dataPage,
    unsigned * pagePos,
    uint32_t id,
    TimePoint time
) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    if (!mi.lastPage)
        return false;
    if (mi.firstPageTime == TimePoint{}) {
        auto dp = viewPage<DataPage>(mi.lastPage);
        mi.firstPageTime = dp->firstPageTime;
        mi.lastPageValue = dp->lastPageValue;
    }
    
    auto lastValueTime = mi.firstPageTime + mi.lastPageValue * mi.interval;

    time -= time.time_since_epoch() % mi.interval;
    auto mp = viewPage<MetricPage>(mi.infoPage);

    if (time >= mi.firstPageTime) {
        if (time > lastValueTime)
            return false;
        *dataPage = mi.lastPage;
        *pagePos = mp->lastPagePos;
        return true;
    }

    if (time <= lastValueTime - mp->retention) {
        // before first value
        return false;
    }
    auto pageInterval = valuesPerPage() * mi.interval;
    auto off = (mi.firstPageTime - time - mi.interval) / pageInterval + 1;
    auto pages = (mp->retention + pageInterval - mi.interval) / pageInterval;
    *pagePos = (uint32_t) (mp->lastPagePos + pages - off) % pages;
    if (!radixFind(dataPage, mi.infoPage, *pagePos))
        *dataPage = 0;
    return true;
}

//===========================================================================
size_t TsdFile::enumValues(
    ITsdEnumNotify * notify, 
    uint32_t id, 
    TimePoint first, 
    TimePoint last
) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    // round time to metric's sampling interval
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    if (first > last)
        return 0;
    
    uint32_t dpno;
    unsigned dppos;
    bool found = findDataPage(&dpno, &dppos, id, first);
    if (!found && first >= mi.firstPageTime)
        return 0;

    auto mp = viewPage<MetricPage>(mi.infoPage);
    auto lastValueTime = mi.firstPageTime + mi.lastPageValue * mi.interval;
    if (last > lastValueTime)
        last = lastValueTime;

    if (!found) {
        if (first < last)
            first = lastValueTime - mp->retention + mi.interval;
        if (first > last)
            return 0;
        found = findDataPage(&dpno, &dppos, id, first);
        assert(found);
    }

    auto name = string_view(mp->name);
    auto vpp = valuesPerPage();
    auto pageInterval = vpp * mi.interval;
    auto numValues = mp->retention / mp->interval;
    auto numPages = (numValues - 1) / vpp + 1;

    unsigned count = 0;
    for (;;) {
        if (!dpno) {
            // round up to first time on next page
            first -= pageInterval - mi.interval;
            auto pageOff = (mi.firstPageTime - first) / pageInterval - 1;
            first = mi.firstPageTime - pageOff * pageInterval;
        } else {
            auto dp = viewPage<DataPage>(dpno);
            auto fpt = dp->firstPageTime;
            auto vpos = (first - fpt) / mi.interval;
            auto lastPageValue = dp->lastPageValue == vpp
                ? vpp - 1
                : dp->lastPageValue;
            auto lastPageTime = fpt + lastPageValue * mi.interval;
            if (vpos < 0) {
                // in the old section of the tip page in the ring buffer
                vpos += numPages * vpp;
                vpos = vpos % vpp;
                assert(vpos);
                lastPageTime = fpt - (numPages - 1) * pageInterval 
                    - mi.interval;
            }
            if (last < lastPageTime) 
                lastPageTime = last;
            for (; first <= lastPageTime; first += mi.interval, ++vpos) {
                auto value = dp->values[vpos];
                if (!isnan(value)) {
                    count += 1;
                    if (!notify->OnTsdValue(id, name, first, value))
                        return count;
                }
            }
        }
        if (first > last)
            break;

        // advance to next page
        dppos = (dppos + 1) % numPages;
        radixFind(&dpno, mi.infoPage, dppos);
    }
    return count;
}


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
static RadixData * radixData(PageHeader * hdr) {
    if (hdr->type == kPageTypeMetric) {
        return &reinterpret_cast<MetricPage *>(hdr)->rd;
    } else {
        assert(hdr->type == kPageTypeRadix);
        return &reinterpret_cast<RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
static const RadixData * radixData(const PageHeader * hdr) {
    return radixData(const_cast<PageHeader *>(hdr));
}

//===========================================================================
void TsdFile::radixFreePage(uint32_t pgno) {
    auto rp = viewPage<RadixPage>(pgno);
    for (int i = 0; i < rp->rd.numPages; ++i) {
        if (uint32_t p = rp->rd.pages[i]) 
            freePage(p);
    }
}

//===========================================================================
void TsdFile::radixClear(PageHeader & hdr) {
    auto rd = radixData(&hdr);
    for (int i = 0; i < rd->numPages; ++i) {
        if (uint32_t p = rd->pages[i]) {
            freePage(p);
            rd->pages[i] = 0;
        }
    }
    rd->height = 0;
}

//===========================================================================
void TsdFile::radixErase(
    PageHeader & rhdr, 
    size_t firstPos, 
    size_t lastPos
) {
    assert(firstPos <= lastPos);
    while (firstPos < lastPos) {
        const PageHeader * hdr;
        const RadixData * rd;
        size_t rpos;
        if (!radixFind(&hdr, &rd, &rpos, rhdr.pgno, firstPos))
            return;

        unique_ptr<PageHeader> nhdr;
        RadixData * nrd;
        if (hdr == &rhdr) {
            nrd = radixData(&rhdr);
        } else {
            nhdr = editPage(*hdr);
            nrd = radixData(nhdr.get());
        }
        auto lastPagePos = min(
            (size_t) nrd->numPages, 
            rpos + lastPos - firstPos
        );
        for (auto i = rpos; i < lastPagePos; ++i, ++firstPos) {
            if (auto p = nrd->pages[i]) {
                freePage(p);
                nrd->pages[i] = 0;
            }
        }
        if (nhdr)
            writePage(*nhdr, m_hdr->pageSize);
    }
}

//===========================================================================
bool TsdFile::radixFind(
    PageHeader const ** hdr, 
    RadixData const ** rd, 
    size_t * rpos,
    uint32_t root, 
    size_t pos
) {
    *hdr = viewPage<PageHeader>(root);
    *rd = radixData(*hdr);
    RadixDigits & cvt = ((*hdr)->type == kPageTypeMetric) 
        ? m_rdMetric 
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    if ((*rd)->height < count)
        return false;
    int * d = digits;
    while (auto height = (*rd)->height) {
        int pos = (height > count) ? 0 : *d;
        if (!(*rd)->pages[pos])
            return false;
        *hdr = viewPage<PageHeader>((*rd)->pages[pos]);
        *rd = radixData(*hdr);
        assert((*rd)->height == height - 1);
        if (height == count) {
            d += 1;
            count -= 1;
        }
    }

    *rpos = *d;
    return true;
}

//===========================================================================
bool TsdFile::radixFind(uint32_t * out, uint32_t root, size_t pos) {
    const PageHeader * hdr;
    const RadixData * rd;
    size_t rpos;
    if (radixFind(&hdr, &rd, &rpos, root, pos)) {
        *out = rd->pages[rpos];
    } else {
        *out = 0;
    }
    return *out;
}

//===========================================================================
bool TsdFile::radixInsert(uint32_t root, size_t pos, uint32_t value) {
    auto hdr = viewPage<PageHeader>(root);
    auto rd = radixData(hdr);
    RadixDigits & cvt = (hdr->type == kPageTypeMetric) 
        ? m_rdMetric 
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    while (rd->height < count) {
        auto mid = allocPage<RadixPage>();
        mid->rd.height = rd->height;
        mid->rd.numPages = (uint16_t) cvt.pageEntries();
        memcpy(mid->rd.pages, rd->pages, rd->numPages * sizeof(rd->pages[0]));
        writePage(*mid, m_hdr->pageSize);

        auto nhdr = editPage(*hdr);
        auto nrd = radixData(nhdr.get());
        nrd->height += 1;
        memset(nrd->pages, 0, nrd->numPages * sizeof(nrd->pages[0]));
        nrd->pages[0] = mid->hdr.pgno;
        writePage(*nhdr, m_hdr->pageSize);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        if (!rd->pages[pos]) {
            auto next = allocPage<RadixPage>();
            next->rd.height = rd->height - 1;
            next->rd.numPages = (uint16_t) cvt.pageEntries();
            writePage(*next);
            auto nhdr = editPage(*hdr);
            auto nrd = radixData(nhdr.get());
            nrd->pages[pos] = next->hdr.pgno;
            writePage(*nhdr, m_hdr->pageSize);
            assert(rd->pages[pos]);
        }
        hdr = viewPage<PageHeader>(rd->pages[pos]);
        rd = radixData(hdr);
        d += 1;
        count -= 1;
    }
    if (rd->pages[*d]) 
        return false;

    auto nhdr = editPage(*hdr);
    auto nrd = radixData(nhdr.get());
    nrd->pages[*d] = value;
    writePage(*nhdr, m_hdr->pageSize);
    return true;
}


/****************************************************************************
*
*   Page management
*
***/

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
        auto fp = viewPage<FreePage>(pgno);
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
bool TsdFile::loadFreePages () {
    auto pgno = m_hdr->freePageRoot;
    size_t num = 0;
    UnsignedSet found;
    while (pgno) {
        auto p = viewPage<PageHeader>(pgno);
        if (!p || p->type != kPageTypeFree)
            return false;
        num += 1;
        found.insert(pgno);
        auto fp = reinterpret_cast<const FreePage*>(p);
        pgno = fp->nextPage;
    }
    return num == found.size();
}

//===========================================================================
void TsdFile::freePage(uint32_t pgno) {
    assert(pgno < m_hdr->numPages);
    auto p = viewPage<PageHeader>(pgno);
    assert(p->type != kPageTypeFree);
    FreePage fp;
    fp.hdr = *p;
    switch (fp.hdr.type) {
    case kPageTypeMetric:
        metricFreePage(pgno);
        break;
    case kPageTypeRadix:
        radixFreePage(pgno);
        break;
    case kPageTypeData:
    case kPageTypeLeaf:
        break;
    case kPageTypeFree:
        logMsgCrash() << "freePage: page already free";
    default:
    case kPageTypeBranch:
        logMsgCrash() << "freePage(" << fp.hdr.type << "): invalid state";
    }
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
    return editPage(*viewPage<T>(pgno));
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
    return dupPage(*viewPage<T>(pgno));
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
const T * TsdFile::viewPage(uint32_t pgno) const {
    assert(pgno < m_hdr->numPages);
    const void * vptr = (char *) m_hdr + m_hdr->pageSize * pgno;
    auto ptr = static_cast<const T*>(vptr);
    assert(ptr->hdr.type == ptr->type);
    return ptr;
}

//===========================================================================
template<>
const PageHeader * TsdFile::viewPage<PageHeader>(uint32_t pgno) const {
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
template<>
void TsdFile::writePage<PageHeader>(PageHeader & hdr, size_t count) const {
    writePage(hdr.pgno, &hdr, count);
}

//===========================================================================
void TsdFile::writePage(uint32_t pgno, const void * ptr, size_t count) const {
    assert(pgno < m_hdr->numPages);
    assert(count <= m_hdr->pageSize);
    fileWriteWait(m_data, pgno * m_hdr->pageSize, ptr, count);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
TsdFileHandle tsdOpen(string_view name, size_t pageSize) {
    auto tsd = make_unique<TsdFile>();
    if (!tsd->open(name, pageSize))
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
void tsdEraseMetric(TsdFileHandle h, uint32_t id) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->eraseMetric(id);
}

//===========================================================================
void tsdUpdateMetric(
    TsdFileHandle h,
    uint32_t id,
    Duration retention,
    Duration interval
) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->updateMetric(id, retention, interval);
}

//===========================================================================
void tsdUpdateValue(
    TsdFileHandle h, 
    uint32_t id, 
    TimePoint time, 
    float value
) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->updateValue(id, time, value);
}

//===========================================================================
void tsdFindMetrics(
    Dim::UnsignedSet & out,
    TsdFileHandle h,
    std::string_view name
) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    tsd->findMetrics(out, name);
}

//===========================================================================
size_t tsdEnumValues(
    ITsdEnumNotify * notify,
    TsdFileHandle h,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    auto * tsd = s_files.find(h);
    assert(tsd);
    return tsd->enumValues(notify, id, first, last);
}
