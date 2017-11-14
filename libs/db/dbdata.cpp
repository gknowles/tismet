// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdata.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const Duration kDefaultRetention = 7 * 24h;
const Duration kDefaultInterval = 1min;

const unsigned kMaxMetricNameLen = 64;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());

const unsigned kMinPageSize = 128;
const unsigned kDefaultPageSize = 4096;
static_assert(kDefaultPageSize == pow2Ceil(kDefaultPageSize));

// Must be a multiple of fileViewAlignment()
const size_t kViewSize = 0x100'0000; // 16MiB
const size_t kDefaultFirstViewSize = 2 * kViewSize;

const uint32_t kMasterPageNum = 0;
const uint32_t kMetricIndexPageNum = 1;


/****************************************************************************
*
*   Private
*
***/

const unsigned kDataFileSig[] = {
    0x39515728,
    0x4873456d,
    0xf6bfd8a1,
    0xa33f3ba2
};

namespace {
enum PageType : uint32_t {
    kPageTypeFree = 'F',
    kPageTypeMaster = 'M',
    kPageTypeSegment = 'S',
    kPageTypeMetric = 'm',
    kPageTypeRadix = 'r',
    kPageTypeSample = 's',
};

struct PageHeader {
    PageType type;
    uint32_t pgno;
    uint32_t id;
    uint32_t checksum;
    uint64_t lsn;
};

struct SegmentPage {
    static const PageType type = kPageTypeSegment;
    PageHeader hdr;
};

struct MasterPage {
    static const PageType type = kPageTypeMaster;
    SegmentPage segment;
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned segmentSize;
};
static_assert(is_standard_layout_v<MasterPage>);
static_assert(2 * sizeof(MasterPage) <= kMinPageSize);

struct FreePage {
    static const PageType type = kPageTypeFree;
    PageHeader hdr;
    unsigned nextPage;
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
    Duration interval;
    Duration retention;
    uint32_t lastPage;
    unsigned lastPagePos;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd;
};

struct SamplePage {
    static const PageType type = kPageTypeSample;
    PageHeader hdr;

    // time of first sample on page
    TimePoint pageFirstTime;

    // Position of last sample, samples that come after this position on the
    // page are either in the not yet populated future or (because it's a
    // giant discontinuous ring buffer) in the distant past.
    uint16_t pageLastSample;

    // EXTENDS BEYOND END OF STRUCT
    float samples[1];
};

struct MetricInfo {
    Duration interval;
    uint32_t infoPage;
    uint32_t lastPage; // page with most recent samples
    TimePoint pageFirstTime; // time of first sample on last page
    uint16_t pageLastSample; // position of last sample on last page
};

class DbFile : public HandleContent {
public:
    ~DbFile();

    bool open(string_view name, size_t pageSize);
    DbStats queryStats();

    bool insertMetric(uint32_t & out, const string & name);
    void eraseMetric(uint32_t id);
    void updateMetric(uint32_t id, Duration retention, Duration interval);

    bool findMetric(uint32_t & out, const string & name) const;
    void findMetrics(UnsignedSet & out, string_view name) const;

    void updateSample(uint32_t id, TimePoint time, float value);
    size_t enumSamples(
        IDbEnumNotify * notify,
        uint32_t id,
        TimePoint first,
        TimePoint last
    );

private:
    bool loadMetrics(uint32_t pgno);
    void metricFreePage(DbTxn & txn, uint32_t pgno);

    bool loadFreePages();
    uint32_t allocPgno(DbTxn & txn);
    template<typename T>
    unique_ptr<T> allocPage(DbTxn & txn, uint32_t id);
    template<typename T>
    unique_ptr<T> allocPage(uint32_t id, uint32_t pgno);
    void freePage(DbTxn & txn, uint32_t pgno);

    const void * viewPageRaw(uint32_t pgno) const;
    template<typename T> const T * viewPage(uint32_t pgno) const;

    // get copy of page to update and then write
    template<typename T> unique_ptr<T> editPage(uint32_t pgno) const;
    template<typename T> unique_ptr<T> editPage(const T & data) const;

    // allocate new page (with new id) that is a copy of an existing page
    template<typename T> unique_ptr<T> dupPage(uint32_t pgno);
    template<typename T> unique_ptr<T> dupPage(const T & data);

    template<typename T>
    void writePage(T & data) const;
    void writePagePrefix(
        uint32_t pgno,
        const void * ptr,
        size_t count
    ) const;

    void radixClear(DbTxn & txn, PageHeader & hdr);
    void radixErase(
        DbTxn & txn,
        PageHeader & hdr,
        size_t firstPos,
        size_t lastPos
    );
    void radixFreePage(DbTxn & txn, uint32_t pgno);
    bool radixInsert(DbTxn & txn, uint32_t root, size_t pos, uint32_t value);
    bool radixFind(
        PageHeader const ** hdr,
        RadixData const ** rd,
        size_t * rpos,
        uint32_t root,
        size_t pos
    );
    bool radixFind(uint32_t * out, uint32_t root, size_t pos);

    size_t samplesPerPage() const;
    unique_ptr<SamplePage> allocSamplePage(
        DbTxn & txn,
        uint32_t id,
        TimePoint time,
        uint16_t lastSample
    );
    MetricInfo & loadMetricInfo(uint32_t id);
    MetricInfo & loadMetricInfo(DbTxn & txn, uint32_t id, TimePoint time);
    bool findSamplePage(
        uint32_t * pgno,
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
        size_t count = 0;
    };

    // metric ids by name length as measured in segments
    vector<UnsignedSetWithCount> m_lenIds;

    // Index of metric ids by the segments of their names. So the wildcard
    // *.red.* could be matched by finding all the metrics whose name has
    // "red" as the second segment (m_segIds[1]["red"]) and three segments
    // long (m_lenIds[3]).
    vector<unordered_map<string, UnsignedSetWithCount>> m_segIds;

    DbRadix m_rdIndex;
    DbRadix m_rdMetric;

    FileHandle m_fdata;
    DbReadView m_vdata;
    const MasterPage * m_hdr = nullptr;
    size_t m_pageSize = 0;
    size_t m_numPages = 0;
    UnsignedSet m_freePages;

    DbLog m_log;
    FileHandle m_fwork;
};
} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<DbHandle, DbFile> s_files;

static auto & s_perfCount = uperf("metrics (total)");
static auto & s_perfCreated = uperf("metrics created");
static auto & s_perfDeleted = uperf("metrics deleted");

static auto & s_perfOld = uperf("samples ignored (old)");
static auto & s_perfDup = uperf("samples ignored (same)");
static auto & s_perfChange = uperf("samples changed");
static auto & s_perfAdd = uperf("samples added");


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
constexpr uint32_t pagesPerSegment(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return uint32_t(CHAR_BIT * pageSize / 2);
}

//===========================================================================
constexpr size_t segmentSize(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return pageSize * pagesPerSegment(pageSize);
}

//===========================================================================
constexpr pair<uint32_t, size_t> segmentPage(uint32_t pgno, size_t pageSize) {
    auto pps = pagesPerSegment(pageSize);
    auto segPage = pgno / pps * pps;
    auto segPos = pgno % pps;
    return {(uint32_t) segPage, segPos};
}


/****************************************************************************
*
*   DbFile
*
***/

//===========================================================================
DbFile::~DbFile () {
    s_perfCount -= (unsigned) m_ids.size();

    m_vdata.close();
    fileClose(m_fdata);

    fileClose(m_fwork);
}

//===========================================================================
bool DbFile::open(string_view name, size_t pageSize) {
    assert(pageSize == pow2Ceil(pageSize));
    if (!pageSize)
        pageSize = kDefaultPageSize;
    assert(kViewSize % fileViewAlignment() == 0);

    m_fdata = fileOpen(
        name,
        File::fCreat | File::fReadWrite | File::fDenyWrite | File::fBlocking
    );
    if (!m_fdata)
        return false;
    auto len = fileSize(m_fdata);
    MasterPage tmp = {};
    if (!len) {
        tmp.segment.hdr.type = kPageTypeMaster;
        auto segSize = segmentSize(pageSize);
        assert(segSize <= numeric_limits<decltype(tmp.segmentSize)>::max());
        tmp.segmentSize = (unsigned) segSize;
        memcpy(tmp.signature, kDataFileSig, sizeof(tmp.signature));
        tmp.pageSize = (unsigned) pageSize;
        fileWriteWait(m_fdata, 0, &tmp, sizeof(tmp));
        vector<uint64_t> freemap;
        freemap.assign(pageSize / 2 / sizeof(uint64_t), 0xffff'ffff'ffff'ffff);
        freemap[0] = 0xffff'ffff'ffff'fffe;
        fileWriteWait(m_fdata, pageSize / 2, freemap.data(), pageSize / 2);
        len = pageSize;
    } else {
        fileReadWait(&tmp, sizeof(tmp), m_fdata, 0);
        pageSize = tmp.pageSize;
    }
    if (memcmp(tmp.signature, kDataFileSig, sizeof(tmp.signature)) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }
    m_pageSize = tmp.pageSize;
    if (m_pageSize < kMinPageSize || kViewSize % m_pageSize != 0) {
        logMsgError() << "Invalid page size in " << name;
        return false;
    }
    if (tmp.segmentSize != segmentSize(m_pageSize)) {
        logMsgError() << "Invalid segment size in " << name;
        return false;
    }
    m_numPages = len / pageSize;

    if (!m_vdata.open(m_fdata, kViewSize, m_pageSize)) {
        logMsgError() << "Open view failed on " << name;
        return false;
    }
    m_hdr = (const MasterPage *) m_vdata.rptr(0);

    auto ipOff = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    m_rdIndex.init(m_pageSize, ipOff, ipOff);
    auto mpOff = offsetof(MetricPage, rd) + offsetof(RadixData, pages);
    m_rdMetric.init(m_pageSize, mpOff, ipOff);

    if (!loadFreePages())
        return false;
    if (m_numPages == 1) {
        auto txn = DbTxn{m_log};
        auto rp = allocPage<RadixPage>(txn, 0);
        assert(rp->hdr.pgno == kMetricIndexPageNum);
        rp->rd.height = 0;
        rp->rd.numPages = (uint16_t) m_rdIndex.rootEntries();
        txn.logInitRadix(rp->hdr.pgno, 0, nullptr, nullptr);
        writePage(*rp);
    }
    if (!loadMetrics(kMetricIndexPageNum))
        return false;

    return true;
}

//===========================================================================
DbStats DbFile::queryStats() {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.segmentSize = (unsigned) m_hdr->segmentSize;
    s.viewSize = kViewSize;
    s.metricNameLength = sizeof(MetricPage::name);
    s.samplesPerPage = (unsigned) samplesPerPage();
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_freePages.size();
    s.metricIds = 0;
    for (auto & len : m_lenIds)
        s.metricIds += (unsigned) len.count;
    return s;
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void DbFile::metricFreePage (DbTxn & txn, uint32_t pgno) {
    auto mp = viewPage<MetricPage>(pgno);
    for (int i = 0; i < mp->rd.numPages; ++i) {
        if (auto pn = mp->rd.pages[i])
            freePage(txn, pn);
    }
    m_metricInfo[mp->hdr.id] = {};
    indexEraseMetric(mp->hdr.id, mp->name);
}

//===========================================================================
bool DbFile::loadMetrics (uint32_t pgno) {
    if (!pgno)
        return true;

    auto p = viewPage<PageHeader>(pgno);
    if (!p)
        return false;

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < rp->rd.numPages; ++i) {
            if (!loadMetrics(rp->rd.pages[i]))
                return false;
        }
        return true;
    }

    if (p->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<const MetricPage*>(p);

        indexInsertMetric(mp->hdr.id, mp->name);

        if (m_metricInfo.size() <= mp->hdr.id)
            m_metricInfo.resize(mp->hdr.id + 1);
        auto & mi = m_metricInfo[mp->hdr.id];
        mi.infoPage = mp->hdr.pgno;
        mi.interval = mp->interval;
        mi.lastPage = mp->lastPage;

        s_perfCount += 1;
        return true;
    }

    return false;
}

//===========================================================================
bool DbFile::findMetric(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end())
        return false;
    out = i->second;
    return true;
}

//===========================================================================
void DbFile::findMetrics(UnsignedSet & out, string_view name) const {
    if (name.empty()) {
        out = m_ids;
        return;
    }

    QueryInfo qry;
    bool result [[maybe_unused]] = queryParse(qry, name);
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
void DbFile::indexInsertMetric(uint32_t id, const string & name) {
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
void DbFile::indexEraseMetric(uint32_t id, const string & name) {
    auto num [[maybe_unused]] = m_metricIds.erase(name);
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
bool DbFile::insertMetric(uint32_t & out, const string & name) {
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
    auto txn = DbTxn{m_log};
    auto mp = allocPage<MetricPage>(txn, id);
    auto count = name.copy(mp->name, size(mp->name) - 1);
    mp->name[count] = 0;
    mp->interval = kDefaultInterval;
    mp->retention = kDefaultRetention;
    mp->rd.height = 0;
    mp->rd.numPages = (uint16_t) m_rdMetric.rootEntries();
    txn.logInitMetric(
        mp->hdr.pgno,
        mp->name,
        mp->retention,
        mp->interval
    );
    writePage(*mp);

    if (id >= m_metricInfo.size())
        m_metricInfo.resize(id + 1);
    auto & mi = m_metricInfo[id];
    assert(!mi.infoPage);
    mi = {};
    mi.infoPage = mp->hdr.pgno;
    mi.interval = mp->interval;

    // update index
    bool inserted [[maybe_unused]] = radixInsert(
        txn,
        kMetricIndexPageNum,
        id,
        mp->hdr.pgno
    );
    assert(inserted);
    s_perfCount += 1;
    return true;
}

//===========================================================================
void DbFile::eraseMetric(uint32_t id) {
    if (uint32_t pgno = m_metricInfo[id].infoPage) {
        auto txn = DbTxn{m_log};
        metricFreePage(txn, pgno);
    }
}

//===========================================================================
void DbFile::updateMetric(
    uint32_t id,
    Duration retention,
    Duration interval
) {
    // TODO: validate interval and retention

    auto & mi = m_metricInfo[id];
    auto mp = viewPage<MetricPage>(mi.infoPage);
    if (mp->retention == retention && mp->interval == interval)
        return;

    auto txn = DbTxn{m_log};
    auto nmp = editPage(*mp);
    nmp->retention = retention;
    nmp->interval = interval;
    radixClear(txn, nmp->hdr);
    nmp->lastPage = 0;
    nmp->lastPagePos = 0;
    writePage(*nmp);
    mi.interval = interval;
    mi.lastPage = 0;
    mi.pageFirstTime = {};
    mi.pageLastSample = 0;
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
size_t DbFile::samplesPerPage() const {
    return (m_pageSize - offsetof(SamplePage, samples))
        / sizeof(SamplePage::samples[0]);
}

//===========================================================================
unique_ptr<SamplePage> DbFile::allocSamplePage(
    DbTxn & txn,
    uint32_t id,
    TimePoint time,
    uint16_t lastSample
) {
    auto vpp = samplesPerPage();
    auto dp = allocPage<SamplePage>(txn, id);
    dp->pageLastSample = lastSample;
    dp->pageFirstTime = time;
    for (auto i = 0; i < vpp; ++i)
        dp->samples[i] = NAN;
    return dp;
}

//===========================================================================
MetricInfo & DbFile::loadMetricInfo(uint32_t id) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    // Update metric info from sample page if it has no page data.
    if (mi.lastPage && mi.pageFirstTime == TimePoint{}) {
        auto dp = viewPage<SamplePage>(mi.lastPage);
        mi.pageFirstTime = dp->pageFirstTime;
        mi.pageLastSample = dp->pageLastSample;
    }
    return mi;
}

//===========================================================================
MetricInfo & DbFile::loadMetricInfo(DbTxn & txn, uint32_t id, TimePoint time) {
    auto & mi = m_metricInfo[id];
    assert(mi.infoPage);

    if (!mi.lastPage) {
        // metric has no sample pages
        // create empty page that covers the requested time

        // round time down to metric's sampling interval
        time -= time.time_since_epoch() % mi.interval;

        auto lastSample = (uint16_t) (id % samplesPerPage());
        auto pageTime = time - lastSample * mi.interval;
        auto dp = allocSamplePage(txn, id, pageTime, lastSample);
        writePage(*dp);

        auto mp = editPage<MetricPage>(mi.infoPage);
        mp->lastPage = dp->hdr.pgno;
        assert(mp->lastPagePos == 0);
        mp->rd.pages[0] = mp->lastPage;
        writePage(*mp);

        mi.lastPage = mp->lastPage;
        mi.pageFirstTime = dp->pageFirstTime;
        mi.pageLastSample = dp->pageLastSample;
    }

    return loadMetricInfo(id);
}

//===========================================================================
void DbFile::updateSample(uint32_t id, TimePoint time, float value) {
    assert(time != TimePoint{});

    auto txn = DbTxn{m_log};

    // ensure all info about the last page is loaded, the expectation is that
    // almost all updates are to the last page.
    auto & mi = loadMetricInfo(txn, id, time);

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto spp = samplesPerPage();
    auto pageInterval = spp * mi.interval;
    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;

    // one interval past last time on page (aka first time on next page)
    auto endPageTime = mi.pageFirstTime + pageInterval;

    // updating historical sample?
    if (time <= lastSampleTime) {
        auto dpno = mi.lastPage;
        auto ent = numeric_limits<uint64_t>::max();
        if (time < mi.pageFirstTime) {
            auto mp = viewPage<MetricPage>(mi.infoPage);
            auto firstSampleTime = lastSampleTime - mp->retention + mi.interval;
            if (time < firstSampleTime) {
                // before first sample
                s_perfOld += 1;
                return;
            }
            auto off = (mi.pageFirstTime - time - mi.interval)
                / pageInterval + 1;
            auto dpages = (mp->retention + pageInterval - mi.interval)
                / pageInterval;
            auto pagePos =
                (uint32_t) (mp->lastPagePos + dpages - off) % dpages;
            if (pagePos == mp->lastPagePos) {
                // Still on the tip page of the ring buffer, but in the old
                // samples section.
                auto pageTime = mi.pageFirstTime - off * pageInterval;
                ent = (time - pageTime) / mi.interval;
            } else if (!radixFind(&dpno, mi.infoPage, pagePos)) {
                auto pageTime = mi.pageFirstTime - off * pageInterval;
                auto dp = allocSamplePage(txn, id, pageTime, (uint16_t) spp - 1);
                writePage(*dp);
                dpno = dp->hdr.pgno;
                bool inserted [[maybe_unused]] = radixInsert(
                    txn,
                    mi.infoPage,
                    pagePos,
                    dpno
                );
                assert(inserted);
            }
        }
        auto dp = editPage<SamplePage>(dpno);
        if (ent == numeric_limits<uint64_t>::max()) {
            assert(time >= dp->pageFirstTime);
            ent = (time - dp->pageFirstTime) / mi.interval;
        }
        assert(ent < (unsigned) spp);
        auto & ref = dp->samples[ent];
        if (ref == value) {
            s_perfDup += 1;
        } else {
            if (isnan(ref)) {
                s_perfAdd += 1;
            } else {
                s_perfChange += 1;
            }
            ref = value;
            writePage(*dp);
        }
        return;
    }

    //-----------------------------------------------------------------------
    // after last known sample

    // If past the end of the page, check if it's also past the retention of
    // all pages.
    if (time >= endPageTime) {
        auto mp = viewPage<MetricPage>(mi.infoPage);
        // further in the future than the retention period? remove all samples
        // and add as new initial sample.
        if (time >= lastSampleTime + mp->retention) {
            auto nmp = editPage<MetricPage>(mi.infoPage);
            radixClear(txn, nmp->hdr);
            nmp->lastPage = 0;
            nmp->lastPagePos = 0;
            writePage(*nmp);
            mi.lastPage = 0;
            mi.pageFirstTime = {};
            mi.pageLastSample = 0;
            updateSample(id, time, value);
            return;
        }
    }

    // update last page
    auto dp = editPage<SamplePage>(mi.lastPage);
    assert(mi.pageFirstTime == dp->pageFirstTime);
    assert(mi.pageLastSample == dp->pageLastSample);
    auto i = mi.pageLastSample;
    for (;;) {
        i += 1;
        lastSampleTime += mi.interval;
        if (lastSampleTime == endPageTime)
            break;
        if (lastSampleTime == time) {
            s_perfAdd += 1;
            dp->samples[i] = value;
            mi.pageLastSample = dp->pageLastSample = i;
            writePage(*dp);
            return;
        }
        dp->samples[i] = NAN;
    }
    mi.pageLastSample = dp->pageLastSample = i;
    writePage(*dp);

    //-----------------------------------------------------------------------
    // sample is after last page

    // delete pages between last page and the one the sample is on
    auto num = (time - endPageTime) / pageInterval;
    auto mp = editPage<MetricPage>(mi.infoPage);
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / spp + 1;
    auto first = (mp->lastPagePos + 1) % numPages;
    auto last = first + num;
    if (num) {
        endPageTime += num * pageInterval;
        if (last <= numPages) {
            radixErase(txn, mp->hdr, first, last);
        } else {
            last %= numPages;
            radixErase(txn, mp->hdr, first, numPages);
            radixErase(txn, mp->hdr, 0, last);
        }
    }

    // update last page references
    mp->lastPagePos = (unsigned) last;
    radixFind(&mp->lastPage, mi.infoPage, last);
    if (!mp->lastPage) {
        dp = allocSamplePage(txn, id, endPageTime, 0);
        mp->lastPage = dp->hdr.pgno;
        writePage(*mp);
        bool inserted [[maybe_unused]] = radixInsert(
            txn,
            mi.infoPage,
            mp->lastPagePos,
            mp->lastPage
        );
        assert(inserted);
        writePage(*dp);
    } else {
        writePage(*mp);
        dp = editPage<SamplePage>(mp->lastPage);
        dp->pageFirstTime = endPageTime;
        dp->pageLastSample = 0;
        dp->samples[0] = NAN;
        writePage(*dp);
    }

    mi.lastPage = mp->lastPage;
    mi.pageFirstTime = dp->pageFirstTime;
    mi.pageLastSample = dp->pageLastSample;

    // write sample to new last page
    updateSample(id, time, value);
}

//===========================================================================
// Returns false if time is outside of the retention period, or if no
// retention period has been established because there are no samples.
// Otherwise it sets:
//  - outPgno: the page number that contains the time point, or zero if the
//      page is missing. Missing pages can occur when the recorded samples
//      have large gaps that span entire pages.
//  - outPos: the position of the page (whether or not it's missing) of the
//      page within the ring buffer of sample pages for the metric.
bool DbFile::findSamplePage(
    uint32_t * outPgno,
    unsigned * outPos,
    uint32_t id,
    TimePoint time
) {
    auto & mi = loadMetricInfo(id);

    if (!mi.lastPage) {
        // metric has no sample pages (i.e. no samples)
        return false;
    }

    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;

    time -= time.time_since_epoch() % mi.interval;
    auto mp = viewPage<MetricPage>(mi.infoPage);

    if (time >= mi.pageFirstTime) {
        if (time > lastSampleTime)
            return false;
        *outPgno = mi.lastPage;
        *outPos = mp->lastPagePos;
        return true;
    }

    if (time <= lastSampleTime - mp->retention) {
        // before first sample
        return false;
    }
    auto pageInterval = samplesPerPage() * mi.interval;
    auto off = (mi.pageFirstTime - time - mi.interval) / pageInterval + 1;
    auto pages = (mp->retention + pageInterval - mi.interval) / pageInterval;
    *outPos = (uint32_t) (mp->lastPagePos + pages - off) % pages;
    if (!radixFind(outPgno, mi.infoPage, *outPos))
        *outPgno = 0;
    return true;
}

//===========================================================================
size_t DbFile::enumSamples(
    IDbEnumNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    auto & mi = loadMetricInfo(id);

    // round time to metric's sampling interval
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    if (first > last)
        return 0;

    uint32_t dpno;
    unsigned dppos;
    bool found = findSamplePage(&dpno, &dppos, id, first);
    if (!found && first >= mi.pageFirstTime)
        return 0;

    auto mp = viewPage<MetricPage>(mi.infoPage);
    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;
    if (last > lastSampleTime)
        last = lastSampleTime;

    if (!found) {
        if (first < last)
            first = lastSampleTime - mp->retention + mi.interval;
        if (first > last)
            return 0;
        found = findSamplePage(&dpno, &dppos, id, first);
        assert(found);
    }

    auto name = string_view(mp->name);
    auto vpp = samplesPerPage();
    auto pageInterval = vpp * mi.interval;
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / vpp + 1;

    unsigned count = 0;
    for (;;) {
        if (!dpno) {
            // round up to first time on next page
            first -= pageInterval - mi.interval;
            auto pageOff = (mi.pageFirstTime - first) / pageInterval - 1;
            first = mi.pageFirstTime - pageOff * pageInterval;
        } else {
            auto dp = viewPage<SamplePage>(dpno);
            auto fpt = dp->pageFirstTime;
            auto vpos = (first - fpt) / mi.interval;
            auto pageLastSample = dp->pageLastSample == vpp
                ? vpp - 1
                : dp->pageLastSample;
            auto lastPageTime = fpt + pageLastSample * mi.interval;
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
                auto value = dp->samples[vpos];
                if (!isnan(value)) {
                    count += 1;
                    if (!notify->OnDbSample(id, name, first, value))
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
void DbFile::radixFreePage(DbTxn & txn, uint32_t pgno) {
    auto rp = viewPage<RadixPage>(pgno);
    for (int i = 0; i < rp->rd.numPages; ++i) {
        if (uint32_t p = rp->rd.pages[i])
            freePage(txn, p);
    }
}

//===========================================================================
void DbFile::radixClear(DbTxn & txn, PageHeader & hdr) {
    auto rd = radixData(&hdr);
    for (int i = 0; i < rd->numPages; ++i) {
        if (uint32_t p = rd->pages[i]) {
            freePage(txn, p);
            rd->pages[i] = 0;
        }
    }
    rd->height = 0;
}

//===========================================================================
void DbFile::radixErase(
    DbTxn & txn,
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
                freePage(txn, p);
                nrd->pages[i] = 0;
            }
        }
        if (nhdr)
            writePage(*nhdr);
    }
}

//===========================================================================
bool DbFile::radixFind(
    PageHeader const ** hdr,
    RadixData const ** rd,
    size_t * rpos,
    uint32_t root,
    size_t pos
) {
    *hdr = viewPage<PageHeader>(root);
    *rd = radixData(*hdr);
    DbRadix & cvt = ((*hdr)->type == kPageTypeMetric)
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
bool DbFile::radixFind(uint32_t * out, uint32_t root, size_t pos) {
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
bool DbFile::radixInsert(
    DbTxn & txn,
    uint32_t root,
    size_t pos,
    uint32_t value
) {
    auto hdr = viewPage<PageHeader>(root);
    auto rd = radixData(hdr);
    DbRadix & cvt = (hdr->type == kPageTypeMetric)
        ? m_rdMetric
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    while (rd->height < count) {
        auto mid = allocPage<RadixPage>(txn, hdr->id);
        mid->rd.height = rd->height;
        mid->rd.numPages = (uint16_t) cvt.pageEntries();
        memcpy(mid->rd.pages, rd->pages, rd->numPages * sizeof(rd->pages[0]));
        writePage(*mid);

        auto nhdr = editPage(*hdr);
        auto nrd = radixData(nhdr.get());
        nrd->height += 1;
        memset(nrd->pages, 0, nrd->numPages * sizeof(nrd->pages[0]));
        nrd->pages[0] = mid->hdr.pgno;
        writePage(*nhdr);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        if (!rd->pages[pos]) {
            auto next = allocPage<RadixPage>(txn, hdr->id);
            next->rd.height = rd->height - 1;
            next->rd.numPages = (uint16_t) cvt.pageEntries();
            writePage(*next);
            auto nhdr = editPage(*hdr);
            auto nrd = radixData(nhdr.get());
            nrd->pages[pos] = next->hdr.pgno;
            writePage(*nhdr);
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
    writePage(*nhdr);
    return true;
}


/****************************************************************************
*
*   Page management
*
***/

//===========================================================================
static BitView segmentBitView(void * hdr, size_t pageSize) {
    auto base = (uint64_t *) ((char *) hdr + pageSize / 2);
    auto words = pageSize / 2 / sizeof(uint64_t);
    return {base, words};
}

//===========================================================================
const void * DbFile::viewPageRaw(uint32_t pgno) const {
    if (pgno >= m_numPages)
        return nullptr;
    return m_vdata.rptr(pgno);
}

//===========================================================================
template<typename T>
const T * DbFile::viewPage(uint32_t pgno) const {
    assert(pgno < m_numPages);
    auto ptr = static_cast<const T *>(viewPageRaw(pgno));
    if constexpr (!is_same_v<T, PageHeader>) {
        assert(ptr->hdr.type == ptr->type);
    }
    return ptr;
}

//===========================================================================
bool DbFile::loadFreePages () {
    auto pps = pagesPerSegment(m_pageSize);
    assert(m_freePages.empty());
    for (uint32_t pgno = 0; pgno < m_numPages; pgno += pps) {
        auto pp = segmentPage(pgno, m_pageSize);
        auto segPage = pp.first;
        assert(!pp.second);
        auto sp = viewPage<PageHeader>(segPage);
        assert(sp->type == kPageTypeSegment || sp->type == kPageTypeMaster);
        auto bits = segmentBitView(const_cast<PageHeader *>(sp), m_pageSize);
        for (auto first = bits.find(0); first != bits.npos; ) {
            auto last = bits.findZero(first);
            if (last == bits.npos)
                last = pps;
            m_freePages.insert(
                pgno + (unsigned) first,
                pgno + (unsigned) last - 1
            );
            first = bits.find(last);
        }
    }

    // validate that pages in free list are in fact free
    uint32_t blank = 0;
    for (auto && pgno : m_freePages) {
        if (pgno >= m_numPages)
            break;
        auto fp = viewPage<PageHeader>(pgno);
        if (!fp || fp->type && fp->type != kPageTypeFree) {
            logMsgError() << "Bad free page #" << pgno << " in "
                << filePath(m_fdata);
            return false;
        }
        if (fp->type) {
            if (blank) {
                logMsgError() << "Blank data page #" << pgno << " in "
                    << filePath(m_fdata);
                return false;
            }
        } else if (!blank) {
            blank = pgno;
        }
    }
    if (blank && blank < m_numPages) {
        logMsgInfo() << "Trimmed " << m_numPages - blank << " blank pages";
        m_numPages = blank;
    }
    return true;
}

//===========================================================================
uint32_t DbFile::allocPgno (DbTxn & txn) {
    if (m_freePages.empty()) {
        auto [segPage, segPos] = segmentPage((uint32_t) m_numPages, m_pageSize);
        assert(segPage == m_numPages && !segPos);
        (void) segPos;
        m_numPages += 1;
        m_vdata.growToFit(segPage);
        auto nsp = allocPage<SegmentPage>(0, segPage);
        auto bits = segmentBitView(nsp.get(), m_pageSize);
        bits.set();
        bits.reset(0);
        writePage(*nsp);
        auto pps = pagesPerSegment(m_pageSize);
        m_freePages.insert(segPage + 1, segPage + pps - 1);
    }
    auto pgno = *m_freePages.lowerBound(0);
    m_freePages.erase(pgno);

    auto [segPage, segPos] = segmentPage(pgno, m_pageSize);
    auto sp = viewPage<PageHeader>(segPage);
    assert(sp->type == kPageTypeSegment || sp->type == kPageTypeMaster);
    auto nsp = editPage(*sp);
    auto bits = segmentBitView(nsp.get(), m_pageSize);
    assert(bits[segPos]);
    bits.reset(segPos);
    writePage(*nsp);
    if (pgno >= m_numPages) {
        assert(pgno == m_numPages);
        m_numPages += 1;
        m_vdata.growToFit(pgno);
    }

    auto fp [[maybe_unused]] = viewPage<PageHeader>(pgno);
    assert(!fp->type || fp->type == kPageTypeFree);
    return pgno;
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::allocPage(DbTxn & txn, uint32_t id) {
    auto pgno = allocPgno(txn);
    return allocPage<T>(id, pgno);
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::allocPage(uint32_t id, uint32_t pgno) {
    void * vptr = new char[m_pageSize];
    memset(vptr, 0, m_pageSize);
    T * ptr = new(vptr) T;
    ptr->hdr.type = ptr->type;
    ptr->hdr.pgno = pgno;
    ptr->hdr.id = id;
    ptr->hdr.checksum = 0;
    ptr->hdr.lsn = 0;
    return unique_ptr<T>(ptr);
}

//===========================================================================
void DbFile::freePage(DbTxn & txn, uint32_t pgno) {
    assert(pgno < m_numPages);
    auto p = viewPage<PageHeader>(pgno);
    assert(p->type != kPageTypeFree);
    FreePage fp;
    fp.hdr = *p;
    switch (fp.hdr.type) {
    case kPageTypeMetric:
        metricFreePage(txn, pgno);
        break;
    case kPageTypeRadix:
        radixFreePage(txn, pgno);
        break;
    case kPageTypeSample:
        break;
    case kPageTypeFree:
        logMsgCrash() << "freePage: page already free";
    default:
        logMsgCrash() << "freePage(" << fp.hdr.type << "): invalid state";
    }

    fp.hdr.type = kPageTypeFree;
    txn.logFree(pgno);
    writePagePrefix(fp.hdr.pgno, &fp, sizeof(fp));
    m_freePages.insert(pgno);

    auto [segPage, segPos] = segmentPage(pgno, m_pageSize);
    auto sp = viewPage<PageHeader>(segPage);
    assert(sp->type == kPageTypeSegment || sp->type == kPageTypeMaster);
    auto nsp = editPage(*sp);
    auto bits = segmentBitView(nsp.get(), m_pageSize);
    assert(!bits[segPos]);
    bits.set(segPos);
    txn.logUpdateSegment(segPage, pgno, true);
    writePage(*nsp);
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::editPage(uint32_t pgno) const {
    return editPage(*viewPage<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::editPage(const T & data) const {
    void * vptr = new char[m_pageSize];
    T * ptr = new(vptr) T;
    memcpy(ptr, &data, m_pageSize);
    return unique_ptr<T>(ptr);
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::dupPage(uint32_t pgno) {
    return dupPage(*viewPage<T>(pgno));
}

//===========================================================================
template<typename T>
unique_ptr<T> DbFile::dupPage(const T & data) {
    auto ptr = editPage(data);
    ptr->hdr.pgno = allocPgno();
    return ptr;
}

//===========================================================================
template<typename T>
void DbFile::writePage(T & data) const {
    assert(m_pageSize);
    if constexpr (is_same_v<T, PageHeader>) {
        writePagePrefix(data.pgno, &data, m_pageSize);
    } else if constexpr (is_same_v<T, MasterPage>) {
        assert((void *) &data == (void *) &data.segment.hdr);
        writePagePrefix(data.segment.hdr.pgno, &data, m_pageSize);
    } else {
        assert((void *) &data == (void *) &data.hdr);
        writePagePrefix(data.hdr.pgno, &data, m_pageSize);
    }
}

//===========================================================================
void DbFile::writePagePrefix(
    uint32_t pgno,
    const void * ptr,
    size_t count
) const {
    assert(pgno < m_numPages);
    assert(count && count <= m_pageSize);
    fileWriteWait(m_fdata, pgno * m_pageSize, ptr, count);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
DbHandle dbOpen(string_view name, size_t pageSize) {
    auto db = make_unique<DbFile>();
    if (!db->open(name, pageSize))
        return DbHandle{};

    auto h = s_files.insert(db.release());
    return h;
}

//===========================================================================
void dbClose(DbHandle h) {
    s_files.erase(h);
}

//===========================================================================
DbStats dbQueryStats(DbHandle h) {
    auto * db = s_files.find(h);
    assert(db);
    return db->queryStats();
}

//===========================================================================
bool dbFindMetric(uint32_t & out, DbHandle h, string_view name) {
    auto * db = s_files.find(h);
    assert(db);
    return db->findMetric(out, string(name));
}

//===========================================================================
void dbFindMetrics(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view name
) {
    auto * db = s_files.find(h);
    assert(db);
    db->findMetrics(out, name);
}

//===========================================================================
bool dbInsertMetric(uint32_t & out, DbHandle h, string_view name) {
    auto * db = s_files.find(h);
    assert(db);
    return db->insertMetric(out, string(name));
}

//===========================================================================
void dbEraseMetric(DbHandle h, uint32_t id) {
    auto * db = s_files.find(h);
    assert(db);
    db->eraseMetric(id);
}

//===========================================================================
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    Duration retention,
    Duration interval
) {
    auto * db = s_files.find(h);
    assert(db);
    db->updateMetric(id, retention, interval);
}

//===========================================================================
void dbUpdateSample(
    DbHandle h,
    uint32_t id,
    TimePoint time,
    float value
) {
    auto * db = s_files.find(h);
    assert(db);
    db->updateSample(id, time, value);
}

//===========================================================================
size_t dbEnumSamples(
    IDbEnumNotify * notify,
    DbHandle h,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    auto * db = s_files.find(h);
    assert(db);
    return db->enumSamples(notify, id, first, last);
}
