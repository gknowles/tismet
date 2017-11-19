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

const uint32_t kZeroPageNum = 0;
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

enum DbPageType : uint32_t {
    kPageTypeFree = 'F',
    kPageTypeZero = 'dZ',
    kPageTypeSegment = 'S',
    kPageTypeMetric = 'm',
    kPageTypeRadix = 'r',
    kPageTypeSample = 's',
};

struct DbData::SegmentPage {
    static const DbPageType type = kPageTypeSegment;
    DbPageHeader hdr;
};

struct DbData::ZeroPage {
    static const DbPageType type = kPageTypeZero;
    union {
        DbPageHeader hdr;
        SegmentPage segment;
    };
    char signature[sizeof(kDataFileSig)];
    unsigned pageSize;
    unsigned segmentSize;
};
static_assert(is_standard_layout_v<DbData::ZeroPage>);
static_assert(2 * sizeof(DbData::ZeroPage) <= kMinPageSize);

struct DbData::FreePage {
    static const DbPageType type = kPageTypeFree;
    DbPageHeader hdr;
};

struct DbData::RadixData {
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    uint32_t pages[1];
};

struct DbData::RadixPage {
    static const DbPageType type = kPageTypeRadix;
    DbPageHeader hdr;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd;
};

struct DbData::MetricPage {
    static const DbPageType type = kPageTypeMetric;
    DbPageHeader hdr;
    char name[kMaxMetricNameLen];
    Duration interval;
    Duration retention;
    uint32_t lastPage;
    unsigned lastPagePos;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd;
};

struct DbData::SamplePage {
    static const DbPageType type = kPageTypeSample;
    DbPageHeader hdr;

    // time of first sample on page
    TimePoint pageFirstTime;

    // Position of last sample, samples that come after this position on the
    // page are either in the not yet populated future or (because it's a
    // giant discontinuous ring buffer) in the distant past.
    uint16_t pageLastSample;

    // EXTENDS BEYOND END OF STRUCT
    float samples[1];
};


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCount = uperf("metrics (total)");

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
*   DbData
*
***/

//===========================================================================
DbData::~DbData () {
    s_perfCount -= m_numMetrics;
    m_vdata.close();
    fileClose(m_fdata);
}

//===========================================================================
bool DbData::open(
    DbTxn & txn,
    IDbEnumNotify * notify,
    string_view name
) {
    m_pageSize = txn.pageSize();
    auto zp = (const ZeroPage *) txn.viewPage<DbPageHeader>(0);
    if (!zp->hdr.type)
        txn.logZeroInit(kZeroPageNum);

    if (memcmp(zp->signature, kDataFileSig, sizeof(zp->signature)) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }
    if (zp->segmentSize != segmentSize(m_pageSize)) {
        logMsgError() << "Invalid segment size in " << name;
        return false;
    }
    m_numPages = txn.numPages();
    m_segmentSize = zp->segmentSize;

    auto ipOff = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    m_rdIndex.init(m_pageSize, ipOff, ipOff);
    auto mpOff = offsetof(MetricPage, rd) + offsetof(RadixData, pages);
    m_rdMetric.init(m_pageSize, mpOff, ipOff);

    if (!loadFreePages(txn))
        return false;
    if (m_numPages == 1) {
        auto pgno = allocPgno(txn);
        assert(pgno == kMetricIndexPageNum);
        txn.logRadixInit(pgno, 0, 0, nullptr, nullptr);
    }
    if (!loadMetrics(txn, notify, kMetricIndexPageNum))
        return false;

    return true;
}

//===========================================================================
DbStats DbData::queryStats() {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.segmentSize = (unsigned) m_segmentSize;
    s.metricNameLength = sizeof(MetricPage::name);
    s.samplesPerPage = (unsigned) samplesPerPage();
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_freePages.size();
    return s;
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void DbData::metricDestructPage (DbTxn & txn, uint32_t pgno) {
    auto mp = txn.viewPage<MetricPage>(pgno);
    radixDestruct(txn, mp->hdr);

    m_metricPos[mp->hdr.id] = {};
    s_perfCount -= 1;
    m_numMetrics -= 1;
}

//===========================================================================
bool DbData::loadMetrics (
    DbTxn & txn,
    IDbEnumNotify * notify,
    uint32_t pgno
) {
    if (!pgno)
        return true;

    auto p = txn.viewPage<DbPageHeader>(pgno);
    if (!p)
        return false;

    if (p->type == kPageTypeRadix) {
        auto rp = reinterpret_cast<const RadixPage*>(p);
        for (int i = 0; i < rp->rd.numPages; ++i) {
            if (!loadMetrics(txn, notify, rp->rd.pages[i]))
                return false;
        }
        return true;
    }

    if (p->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<const MetricPage*>(p);
        if (notify && !notify->OnDbSample(mp->hdr.id, mp->name, {}, 0))
            return false;

        if (m_metricPos.size() <= mp->hdr.id)
            m_metricPos.resize(mp->hdr.id + 1);
        auto & mi = m_metricPos[mp->hdr.id];
        mi.infoPage = mp->hdr.pgno;
        mi.interval = mp->interval;
        mi.lastPage = mp->lastPage;

        s_perfCount += 1;
        m_numMetrics += 1;
        return true;
    }

    return false;
}

//===========================================================================
void DbData::insertMetric(DbTxn & txn, uint32_t id, const string & name) {
    assert(!name.empty());
    assert(name.size() < kMaxMetricNameLen);

    // set info page
    auto pgno = allocPgno(txn);
    txn.logMetricInit(
        pgno,
        id,
        name,
        kDefaultRetention,
        kDefaultInterval
    );

    auto mp = txn.viewPage<MetricPage>(pgno);
    if (id >= m_metricPos.size())
        m_metricPos.resize(id + 1);
    auto & mi = m_metricPos[id];
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
    m_numMetrics += 1;
}

//===========================================================================
void DbData::applyMetricInit(
    void * ptr,
    uint32_t id,
    string_view name,
    Duration retention,
    Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    mp->hdr.type = mp->type;
    mp->hdr.id = id;
    auto count = name.copy(mp->name, size(mp->name) - 1);
    mp->name[count] = 0;
    mp->retention = retention;
    mp->interval = interval;
    mp->rd.height = 0;
    mp->rd.numPages = (uint16_t) m_rdMetric.rootEntries();
}

//===========================================================================
bool DbData::eraseMetric(DbTxn & txn, string & name, uint32_t id) {
    if (uint32_t pgno = m_metricPos[id].infoPage) {
        name = txn.viewPage<MetricPage>(pgno)->name;
        freePage(txn, pgno);
        return true;
    }
    return false;
}

//===========================================================================
bool DbData::getMetricInfo(DbTxn & txn, MetricInfo & info, uint32_t id) {
    if (id >= m_metricPos.size()) {
        info = {};
        return false;
    }
    auto & mi = m_metricPos[id];
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    info.retention = mp->retention;
    info.interval = mp->interval;
    return true;
}

//===========================================================================
void DbData::updateMetric(
    DbTxn & txn,
    uint32_t id,
    const MetricInfo & info
) {
    assert(id < m_metricPos.size());
    // TODO: validate interval and retention

    auto & mi = m_metricPos[id];
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    if (mp->retention == info.retention && mp->interval == info.interval)
        return;

    radixDestruct(txn, mp->hdr);
    txn.logMetricUpdate(mi.infoPage, info.retention, info.interval);

    mi.interval = info.interval;
    mi.lastPage = 0;
    mi.pageFirstTime = {};
    mi.pageLastSample = 0;
}

//===========================================================================
void DbData::applyMetricUpdate(
    void * ptr,
    Dim::Duration retention,
    Dim::Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->type);
    mp->retention = retention;
    mp->interval = interval;
    mp->lastPage = 0;
    mp->lastPagePos = 0;
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
size_t DbData::samplesPerPage() const {
    return (m_pageSize - offsetof(SamplePage, samples))
        / sizeof(SamplePage::samples[0]);
}

//===========================================================================
DbData::MetricPosition & DbData::loadMetricPos(DbTxn & txn, uint32_t id) {
    auto & mi = m_metricPos[id];
    assert(mi.infoPage);

    // Update metric info from sample page if it has no page data.
    if (mi.lastPage && mi.pageFirstTime == TimePoint{}) {
        auto sp = txn.viewPage<SamplePage>(mi.lastPage);
        mi.pageFirstTime = sp->pageFirstTime;
        mi.pageLastSample = sp->pageLastSample;
    }
    return mi;
}

//===========================================================================
DbData::MetricPosition & DbData::loadMetricPos(
    DbTxn & txn,
    uint32_t id,
    TimePoint time
) {
    auto & mi = m_metricPos[id];
    assert(mi.infoPage);

    if (!mi.lastPage) {
        // metric has no sample pages
        // create empty page that covers the requested time

        // round time down to metric's sampling interval
        time -= time.time_since_epoch() % mi.interval;

        auto lastSample = (uint16_t) (id % samplesPerPage());
        auto pageTime = time - lastSample * mi.interval;
        auto spno = allocPgno(txn);
        txn.logSampleInit(spno, id, pageTime, lastSample);

        txn.logMetricUpdateSamples(
            mi.infoPage,
            0,
            spno,
            true,
            true
        );

        mi.lastPage = spno;
        mi.pageFirstTime = pageTime;
        mi.pageLastSample = lastSample;
    }

    return loadMetricPos(txn, id);
}

//===========================================================================
void DbData::applyMetricClearSamples(void * ptr) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->type);
    mp->lastPage = 0;
    mp->lastPagePos = 0;
    memset(mp->rd.pages, 0, mp->rd.numPages * sizeof(*mp->rd.pages));
}

//===========================================================================
void DbData::applyMetricUpdateSamples(
    void * ptr,
    size_t pos,
    uint32_t refPage,
    bool updateLast,
    bool updateIndex
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->type);
    if (updateLast) {
        mp->lastPage = refPage;
        mp->lastPagePos = (unsigned) pos;
    }
    if (updateIndex)
        mp->rd.pages[pos] = refPage;
}

//===========================================================================
void DbData::updateSample(
    DbTxn & txn,
    uint32_t id,
    TimePoint time,
    float value
) {
    assert(time != TimePoint{});

    // ensure all info about the last page is loaded, the expectation is that
    // almost all updates are to the last page.
    auto & mi = loadMetricPos(txn, id, time);

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto spp = samplesPerPage();
    auto pageInterval = spp * mi.interval;
    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;

    // one interval past last time on page (aka first time on next page)
    auto endPageTime = mi.pageFirstTime + pageInterval;

    // updating historical sample?
    if (time <= lastSampleTime) {
        auto spno = mi.lastPage;
        auto ent = numeric_limits<uint64_t>::max();
        if (time < mi.pageFirstTime) {
            auto mp = txn.viewPage<MetricPage>(mi.infoPage);
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
            } else if (!radixFind(txn, &spno, mi.infoPage, pagePos)) {
                auto pageTime = mi.pageFirstTime - off * pageInterval;
                spno = allocPgno(txn);
                txn.logSampleInit(spno, id, pageTime, (uint16_t) spp - 1);
                bool inserted [[maybe_unused]] = radixInsert(
                    txn,
                    mi.infoPage,
                    pagePos,
                    spno
                );
                assert(inserted);
            }
        }
        auto sp = txn.viewPage<SamplePage>(spno);
        if (ent == numeric_limits<uint64_t>::max()) {
            assert(time >= sp->pageFirstTime);
            ent = (time - sp->pageFirstTime) / mi.interval;
        }
        assert(ent < (unsigned) spp);
        auto ref = sp->samples[ent];
        if (ref == value) {
            s_perfDup += 1;
        } else {
            if (isnan(ref)) {
                s_perfAdd += 1;
            } else {
                s_perfChange += 1;
            }
            txn.logSampleUpdate(spno, ent, ent, value, false);
        }
        return;
    }

    //-----------------------------------------------------------------------
    // after last known sample

    // If past the end of the page, check if it's also past the retention of
    // all pages.
    if (time >= endPageTime) {
        auto mp = txn.viewPage<MetricPage>(mi.infoPage);
        // further in the future than the retention period? remove all samples
        // and add as new initial sample.
        if (time >= lastSampleTime + mp->retention) {
            radixDestruct(txn, mp->hdr);
            txn.logMetricClearSamples(mi.infoPage);
            mi.lastPage = 0;
            mi.pageFirstTime = {};
            mi.pageLastSample = 0;
            updateSample(txn, id, time, value);
            return;
        }
    }

    // update last page
    if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
        auto sp = txn.viewPage<SamplePage>(mi.lastPage);
        assert(mi.pageFirstTime == sp->pageFirstTime);
        assert(mi.pageLastSample == sp->pageLastSample);
    }

    if (time < endPageTime) {
        auto lastSample = (uint16_t) ((time - mi.pageFirstTime) / mi.interval);
        s_perfAdd += 1;
        txn.logSampleUpdate(
            mi.lastPage,
            mi.pageLastSample + 1,
            lastSample,
            value,
            true
        );
        mi.pageLastSample = lastSample;
        return;
    }

    txn.logSampleUpdate(mi.lastPage, mi.pageLastSample + 1, spp, NAN, true);
    mi.pageLastSample = (uint16_t) spp;

    //-----------------------------------------------------------------------
    // sample is after last page

    // delete pages between last page and the one the sample is on
    auto num = (time - endPageTime) / pageInterval;
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
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

    // update reference to last sample page
    uint32_t lastPage;
    if (!radixFind(txn, &lastPage, mi.infoPage, last)) {
        lastPage = allocPgno(txn);
        txn.logSampleInit(lastPage, id, endPageTime, 0);
        bool inserted [[maybe_unused]] = radixInsert(
            txn,
            mi.infoPage,
            last,
            lastPage
        );
        assert(inserted);
    } else {
        txn.logSampleUpdateTime(lastPage, endPageTime);
    }
    txn.logMetricUpdateSamples(mi.infoPage, last, lastPage, true, false);

    mi.lastPage = lastPage;
    mi.pageFirstTime = endPageTime;
    mi.pageLastSample = 0;

    // write sample to new last page
    updateSample(txn, id, time, value);
}

//===========================================================================
void DbData::applySampleInit(
    void * ptr,
    uint32_t id,
    TimePoint pageTime,
    size_t lastSample
) {
    auto sp = static_cast<SamplePage *>(ptr);
    sp->hdr.type = sp->type;
    sp->hdr.id = id;
    sp->pageLastSample = (uint16_t) lastSample;
    sp->pageFirstTime = pageTime;
    auto vpp = samplesPerPage();
    for (auto i = 0; i < vpp; ++i)
        sp->samples[i] = NAN;
}

//===========================================================================
void DbData::applySampleUpdate(
    void * ptr,
    size_t firstPos,
    size_t lastPos,
    float value,
    bool updateLast
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->type);
    for (auto i = firstPos; i < lastPos; ++i)
        sp->samples[i] = NAN;
    if (!isnan(value))
        sp->samples[lastPos] = value;
    if (updateLast)
        sp->pageLastSample = (uint16_t) lastPos;
}

//===========================================================================
void DbData::applySampleUpdateTime(void * ptr, Dim::TimePoint pageTime) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->type);
    sp->pageFirstTime = pageTime;
    sp->pageLastSample = 0;
    sp->samples[0] = NAN;
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
bool DbData::findSamplePage(
    DbTxn & txn,
    uint32_t * outPgno,
    unsigned * outPos,
    uint32_t id,
    TimePoint time
) {
    auto & mi = loadMetricPos(txn, id);

    if (!mi.lastPage) {
        // metric has no sample pages (i.e. no samples)
        return false;
    }

    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;

    time -= time.time_since_epoch() % mi.interval;
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);

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
    if (!radixFind(txn, outPgno, mi.infoPage, *outPos))
        *outPgno = 0;
    return true;
}

//===========================================================================
size_t DbData::enumSamples(
    DbTxn & txn,
    IDbEnumNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    auto & mi = loadMetricPos(txn, id);

    // round time to metric's sampling interval
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    if (first > last)
        return 0;

    uint32_t spno;
    unsigned dppos;
    bool found = findSamplePage(txn, &spno, &dppos, id, first);
    if (!found && first >= mi.pageFirstTime)
        return 0;

    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;
    if (last > lastSampleTime)
        last = lastSampleTime;

    if (!found) {
        if (first < last)
            first = lastSampleTime - mp->retention + mi.interval;
        if (first > last)
            return 0;
        found = findSamplePage(txn, &spno, &dppos, id, first);
        assert(found);
    }

    auto name = string_view(mp->name);
    auto vpp = samplesPerPage();
    auto pageInterval = vpp * mi.interval;
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / vpp + 1;

    unsigned count = 0;
    for (;;) {
        if (!spno) {
            // round up to first time on next page
            first -= pageInterval - mi.interval;
            auto pageOff = (mi.pageFirstTime - first) / pageInterval - 1;
            first = mi.pageFirstTime - pageOff * pageInterval;
        } else {
            auto sp = txn.viewPage<SamplePage>(spno);
            auto fpt = sp->pageFirstTime;
            auto vpos = (first - fpt) / mi.interval;
            auto pageLastSample = sp->pageLastSample == vpp
                ? vpp - 1
                : sp->pageLastSample;
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
                auto value = sp->samples[vpos];
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
        radixFind(txn, &spno, mi.infoPage, dppos);
    }
    return count;
}


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
static DbData::RadixData * radixData(DbPageHeader * hdr) {
    if (hdr->type == kPageTypeMetric) {
        return &reinterpret_cast<DbData::MetricPage *>(hdr)->rd;
    } else {
        assert(hdr->type == kPageTypeRadix);
        return &reinterpret_cast<DbData::RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
static const DbData::RadixData * radixData(const DbPageHeader * hdr) {
    return radixData(const_cast<DbPageHeader *>(hdr));
}

//===========================================================================
void DbData::radixDestructPage(DbTxn & txn, uint32_t pgno) {
    auto rp = txn.viewPage<RadixPage>(pgno);
    radixDestruct(txn, rp->hdr);
}

//===========================================================================
void DbData::radixDestruct(DbTxn & txn, const DbPageHeader & hdr) {
    auto rd = radixData(&hdr);
    for (int i = 0; i < rd->numPages; ++i) {
        if (uint32_t p = rd->pages[i])
            freePage(txn, p);
    }
}

//===========================================================================
void DbData::radixErase(
    DbTxn & txn,
    const DbPageHeader & rhdr,
    size_t firstPos,
    size_t lastPos
) {
    assert(firstPos <= lastPos);
    while (firstPos < lastPos) {
        const DbPageHeader * hdr;
        const RadixData * rd;
        size_t rpos;
        if (!radixFind(txn, &hdr, &rd, &rpos, rhdr.pgno, firstPos))
            return;

        auto lastPagePos = min(
            (size_t) rd->numPages,
            rpos + lastPos - firstPos
        );
        for (auto i = rpos; i < lastPagePos; ++i) {
            if (auto p = rd->pages[i])
                freePage(txn, p);
        }
        auto nextPos = firstPos + lastPagePos - rpos;
        txn.logRadixErase(rhdr.pgno, firstPos, nextPos);
        firstPos = nextPos;
    }
}

//===========================================================================
bool DbData::radixInsert(
    DbTxn & txn,
    uint32_t root,
    size_t pos,
    uint32_t value
) {
    auto hdr = txn.viewPage<DbPageHeader>(root);
    auto rd = radixData(hdr);
    DbRadix & cvt = (hdr->type == kPageTypeMetric)
        ? m_rdMetric
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    while (rd->height < count) {
        auto pgno = allocPgno(txn);
        txn.logRadixInit(
            pgno,
            hdr->id,
            rd->height,
            rd->pages,
            rd->pages + rd->numPages
        );
        txn.logRadixPromote(hdr->pgno, pgno);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        if (!rd->pages[pos]) {
            auto pgno = allocPgno(txn);
            txn.logRadixInit(
                pgno,
                hdr->id,
                rd->height - 1,
                nullptr,
                nullptr
            );
            txn.logRadixUpdate(hdr->pgno, pos, pgno);
            assert(pgno && pgno == rd->pages[pos]);
        }
        hdr = txn.viewPage<DbPageHeader>(rd->pages[pos]);
        rd = radixData(hdr);
        d += 1;
        count -= 1;
    }
    if (rd->pages[*d])
        return false;

    txn.logRadixUpdate(hdr->pgno, *d, value);
    return true;
}

//===========================================================================
void DbData::applyRadixInit(
    void * ptr,
    uint32_t id,
    uint16_t height,
    const uint32_t * firstPgno,
    const uint32_t * lastPgno
) {
    auto rp = static_cast<RadixPage *>(ptr);
    rp->hdr.type = rp->type;
    rp->hdr.id = id;
    rp->rd.height = height;
    rp->rd.numPages = (uint16_t) m_rdIndex.rootEntries();
    if (auto count = lastPgno - firstPgno) {
        assert(count <= rp->rd.numPages);
        memcpy(rp->rd.pages, firstPgno, count * sizeof(*firstPgno));
    }
}

//===========================================================================
void DbData::applyRadixErase(void * ptr, size_t firstPos, size_t lastPos) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == kPageTypeMetric || hdr->type == kPageTypeRadix);
    auto rd = radixData(hdr);
    assert(firstPos < lastPos);
    assert(lastPos <= rd->numPages);
    memset(rd->pages + firstPos, 0, (lastPos - firstPos) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::applyRadixPromote(void * ptr, uint32_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == kPageTypeMetric || hdr->type == kPageTypeRadix);
    auto rd = radixData(hdr);
    rd->height += 1;
    rd->pages[0] = refPage;
    memset(rd->pages + 1, 0, (rd->numPages - 1) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::applyRadixUpdate(void * ptr, size_t pos, uint32_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == kPageTypeMetric || hdr->type == kPageTypeRadix);
    auto rd = radixData(hdr);
    assert(pos < rd->numPages);
    rd->pages[pos] = refPage;
}

//===========================================================================
bool DbData::radixFind(
    DbTxn & txn,
    DbPageHeader const ** hdr,
    RadixData const ** rd,
    size_t * rpos,
    uint32_t root,
    size_t pos
) {
    *hdr = txn.viewPage<DbPageHeader>(root);
    *rd = radixData(*hdr);
    DbRadix & cvt = ((*hdr)->type == kPageTypeMetric)
        ? m_rdMetric
        : m_rdIndex;

    int digits[10];
    size_t count = cvt.convert(digits, size(digits), pos);
    count -= 1;
    if ((*rd)->height < count) {
        // pos is beyond the limit that can be held in a tree this size, in
        // other words, it's past the end.
        return false;
    }
    int * d = digits;
    while (auto height = (*rd)->height) {
        int pos = (height > count) ? 0 : *d;
        if (!(*rd)->pages[pos]) {
            // Any zero value in a non-leaf page (since the stem pages are
            // fully populated up to the highest pos) means that we're past
            // the end.
            return false;
        }
        *hdr = txn.viewPage<DbPageHeader>((*rd)->pages[pos]);
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
bool DbData::radixFind(
    DbTxn & txn,
    uint32_t * out,
    uint32_t root,
    size_t pos
) {
    const DbPageHeader * hdr;
    const RadixData * rd;
    size_t rpos;
    if (radixFind(txn, &hdr, &rd, &rpos, root, pos)) {
        *out = rd->pages[rpos];
    } else {
        *out = 0;
    }
    return *out;
}


/****************************************************************************
*
*   Segments
*
***/

//===========================================================================
static BitView segmentBitView(void * hdr, size_t pageSize) {
    auto base = (uint64_t *) ((char *) hdr + pageSize / 2);
    auto words = pageSize / 2 / sizeof(uint64_t);
    return {base, words};
}

//===========================================================================
bool DbData::loadFreePages (DbTxn & txn) {
    auto pps = pagesPerSegment(m_pageSize);
    assert(m_freePages.empty());
    for (uint32_t pgno = 0; pgno < m_numPages; pgno += pps) {
        auto pp = segmentPage(pgno, m_pageSize);
        auto segPage = pp.first;
        assert(!pp.second);
        auto sp = txn.viewPage<DbPageHeader>(segPage);
        assert(sp->type == kPageTypeSegment || sp->type == kPageTypeZero);
        auto bits = segmentBitView(const_cast<DbPageHeader *>(sp), m_pageSize);
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
        auto fp = txn.viewPage<DbPageHeader>(pgno);
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
uint32_t DbData::allocPgno (DbTxn & txn) {
    if (m_freePages.empty()) {
        auto [segPage, segPos] = segmentPage((uint32_t) m_numPages, m_pageSize);
        assert(segPage == m_numPages && !segPos);
        (void) segPos;
        m_numPages += 1;
        txn.growToFit(segPage);
        txn.logSegmentInit(segPage);
        auto pps = pagesPerSegment(m_pageSize);
        m_freePages.insert(segPage + 1, segPage + pps - 1);
    }
    auto pgno = *m_freePages.lowerBound(0);
    m_freePages.erase(pgno);

    auto segPage = segmentPage(pgno, m_pageSize).first;
    txn.logSegmentUpdate(segPage, pgno, false);
    if (pgno >= m_numPages) {
        assert(pgno == m_numPages);
        m_numPages += 1;
        txn.growToFit(pgno);
    }

    auto fp [[maybe_unused]] = txn.viewPage<DbPageHeader>(pgno);
    assert(!fp->type || fp->type == kPageTypeFree);
    return pgno;
}

//===========================================================================
void DbData::freePage(DbTxn & txn, uint32_t pgno) {
    assert(pgno < m_numPages);
    auto p = txn.viewPage<DbPageHeader>(pgno);
    assert(p->type != kPageTypeFree);
    FreePage fp;
    fp.hdr = *p;
    switch (fp.hdr.type) {
    case kPageTypeMetric:
        metricDestructPage(txn, pgno);
        break;
    case kPageTypeRadix:
        radixDestructPage(txn, pgno);
        break;
    case kPageTypeSample:
        break;
    case kPageTypeFree:
        logMsgCrash() << "freePage: page already free";
    default:
        logMsgCrash() << "freePage(" << fp.hdr.type << "): invalid state";
    }

    txn.logPageFree(pgno);
    m_freePages.insert(pgno);

    auto segPage = segmentPage(pgno, m_pageSize).first;
    txn.logSegmentUpdate(segPage, pgno, true);
}

//===========================================================================
void DbData::applyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    fp->hdr.type = kPageTypeFree;
}

//===========================================================================
void DbData::applyZeroInit(void * ptr) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(zp->hdr.type == 0);
    zp->hdr.type = zp->type;
    zp->hdr.id = 0;
    assert(zp->hdr.pgno == kZeroPageNum);
    auto segSize = segmentSize(m_pageSize);
    assert(segSize <= numeric_limits<decltype(zp->segmentSize)>::max());
    zp->segmentSize = (unsigned) segSize;
    memcpy(zp->signature, kDataFileSig, sizeof(zp->signature));
    zp->pageSize = (unsigned) m_pageSize;
    auto bits = segmentBitView(zp, m_pageSize);
    bits.set();
    bits.reset(0);
}

//===========================================================================
void DbData::applySegmentInit(void * ptr) {
    auto sp = static_cast<SegmentPage *>(ptr);
    assert(sp->hdr.type == 0);
    sp->hdr.type = sp->type;
    sp->hdr.id = 0;
    if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
        auto [segPage, segPos] = segmentPage(sp->hdr.pgno, m_pageSize);
        assert(segPage == sp->hdr.pgno && !segPos);
    }
    auto bits = segmentBitView(sp, m_pageSize);
    bits.set();
    bits.reset(0);
}

//===========================================================================
void DbData::applySegmentUpdate(
    void * ptr,
    uint32_t refPage,
    bool free
) {
    auto sp = static_cast<SegmentPage *>(ptr);
    assert(sp->hdr.type == kPageTypeZero || sp->hdr.type == kPageTypeSegment);
    auto [segPage, segPos] = segmentPage(refPage, m_pageSize);
    assert(sp->hdr.pgno == segPage);
    ignore = segPage;
    auto bits = segmentBitView(sp, m_pageSize);
    assert(bits[segPos] != free);
    bits.set(segPos, free);
}
