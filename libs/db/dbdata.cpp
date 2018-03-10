// Copyright Glen Knowles 2017 - 2018.
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

const DbSampleType kDefaultSampleType = kSampleTypeFloat32;
const Duration kDefaultRetention = 7 * 24h;
const Duration kDefaultInterval = 1min;

const unsigned kMaxMetricNameLen = 128;
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
    0xa33f3ba2,
};

enum DbPageType : int32_t {
    kPageTypeFree = 'F',
    kPageTypeZero = 'dZ',
    kPageTypeSegment = 'S',
    kPageTypeMetric = 'm',
    kPageTypeRadix = 'r',
    kPageTypeSample = 's',
};

struct DbData::SegmentPage {
    static const DbPageType s_pageType = kPageTypeSegment;
    DbPageHeader hdr;
};

struct DbData::ZeroPage {
    static const DbPageType s_pageType = kPageTypeZero;
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
    static const DbPageType s_pageType = kPageTypeFree;
    DbPageHeader hdr;
};

struct DbData::RadixData {
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    uint32_t pages[3];
};

struct DbData::RadixPage {
    static const DbPageType s_pageType = kPageTypeRadix;
    DbPageHeader hdr;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd;
};

struct DbData::MetricPage {
    static const DbPageType s_pageType = kPageTypeMetric;
    DbPageHeader hdr;
    TimePoint creation;
    Duration interval;
    Duration retention;
    TimePoint lastPageFirstTime;
    uint32_t lastPage;
    unsigned lastPagePos;
    DbSampleType sampleType;

    // EXTENDS BEYOND END OF STRUCT
    char name[1];

    // RadixData object immediately follows name
};
static_assert(sizeof(DbData::MetricPage) <= kMinPageSize);

struct DbData::SamplePage {
    static const DbPageType s_pageType = kPageTypeSample;
    DbPageHeader hdr;

    // time of first sample on page
    TimePoint pageFirstTime;

    // Position of last sample, samples that come after this position on the
    // page are either in the not yet populated future or (because it's a
    // giant discontinuous ring buffer) in the distant past.
    uint16_t pageLastSample;
    DbSampleType sampleType;

    // EXTENDS BEYOND END OF STRUCT
    union {
        float f32[1];
        double f64[1];
        int8_t i8[1];
        int16_t i16[1];
        int32_t i32[1];
    } samples;
};


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCount = uperf("db.metrics (total)");

static auto & s_perfOld = uperf("db.samples ignored (old)");
static auto & s_perfDup = uperf("db.samples ignored (same)");
static auto & s_perfChange = uperf("db.samples changed");
static auto & s_perfAdd = uperf("db.samples added");


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

//===========================================================================
constexpr size_t metricNameSize(size_t pageSize) {
    assert(pageSize > sizeof(DbData::MetricPage) + sizeof(DbData::RadixData));
    auto count = pageSize - sizeof(DbData::MetricPage)
        - sizeof(DbData::RadixData);
    if (count > kMaxMetricNameLen)
        count = kMaxMetricNameLen;
    return count;
}

//===========================================================================
constexpr size_t sampleTypeSize(DbSampleType type) {
    switch (type) {
    case kSampleTypeInvalid:
    case kSampleTypes:
        break;
    case kSampleTypeFloat32: return sizeof(float);
    case kSampleTypeFloat64: return sizeof(double);
    case kSampleTypeInt8: return sizeof(int8_t);
    case kSampleTypeInt16: return sizeof(int16_t);
    case kSampleTypeInt32: return sizeof(int32_t);
    }
    assert(!"invalid DbSampleType enum value");
    return 0;
}

//===========================================================================
constexpr size_t samplesPerPage(DbSampleType type, size_t pageSize) {
    return (pageSize - offsetof(DbData::SamplePage, samples))
        / sampleTypeSize(type);
}

//===========================================================================
static void noSamples(
    IDbDataNotify * notify,
    uint32_t id,
    string_view name,
    DbSampleType stype,
    TimePoint first,
    Duration interval
) {
    if (notify) {
        DbSeriesInfo info{};
        info.id = id;
        info.name = name;
        info.type = stype;
        info.first = first;
        info.last = first;
        info.interval = interval;
        if (notify->onDbSeriesStart(info))
            notify->onDbSeriesEnd(id);
    }
}


/****************************************************************************
*
*   DbData
*
***/

//===========================================================================
// static
size_t DbData::queryPageSize(FileHandle f) {
    if (!f)
        return 0;
    ZeroPage zp;
    if (auto bytes = fileReadWait(&zp, sizeof(zp), f, 0); bytes != sizeof(zp))
        return 0;
    if (zp.hdr.type != zp.s_pageType)
        return 0;
    if (memcmp(zp.signature, kDataFileSig, sizeof(zp.signature)) != 0)
        return 0;
    return zp.pageSize;
}

//===========================================================================
DbData::~DbData () {
    s_perfCount -= m_numMetrics;
}

//===========================================================================
void DbData::openForApply(size_t pageSize, DbOpenFlags flags) {
    m_verbose = flags & fDbOpenVerbose;
    m_pageSize = pageSize;
}

//===========================================================================
bool DbData::openForUpdate(
    DbTxn & txn,
    IDbDataNotify * notify,
    string_view name,
    DbOpenFlags flags
) {
    assert(m_pageSize);
    m_verbose = flags & fDbOpenVerbose;

    auto zp = (const ZeroPage *) txn.viewPage<DbPageHeader>(0);
    if (!zp->hdr.type) {
        txn.logZeroInit(kZeroPageNum);
        zp = (const ZeroPage *) txn.viewPage<DbPageHeader>(0);
    }

    if (memcmp(zp->signature, kDataFileSig, sizeof(zp->signature)) != 0) {
        logMsgError() << "Bad signature in " << name;
        return false;
    }
    if (zp->pageSize != m_pageSize) {
        logMsgError() << "Mismatched page size in " << name;
        return false;
    }
    if (zp->segmentSize != segmentSize(m_pageSize)) {
        logMsgError() << "Mismatched segment size in " << name;
        return false;
    }
    m_numPages = txn.numPages();
    m_segmentSize = zp->segmentSize;

    if (m_verbose)
        logMsgInfo() << "Load free page list";
    if (!loadFreePages(txn))
        return false;
    if (m_numPages == 1) {
        auto pgno = allocPgno(txn);
        assert(pgno == kMetricIndexPageNum);
        txn.logRadixInit(pgno, 0, 0, nullptr, nullptr);
    }
    if (m_verbose)
        logMsgInfo() << "Build metric index";
    if (!loadMetrics(txn, notify, kMetricIndexPageNum))
        return false;

    return true;
}

//===========================================================================
DbStats DbData::queryStats() {
    DbStats s;
    s.pageSize = (unsigned) m_pageSize;
    s.segmentSize = (unsigned) m_segmentSize;
    s.metricNameSize = (unsigned) metricNameSize(m_pageSize);
    s.samplesPerPage[kSampleTypeInvalid] = 0;
    for (int8_t i = 1; i < kSampleTypes; ++i)
        s.samplesPerPage[i] = (unsigned) samplesPerPage(DbSampleType{i});

    {
        shared_lock<shared_mutex> lk{m_mposMut};
        s.metrics = m_numMetrics;
    }

    scoped_lock<recursive_mutex> lk{m_pageMut};
    s.numPages = (unsigned) m_numPages;
    s.freePages = (unsigned) m_freePages.size();
    return s;
}

//===========================================================================
DbData::MetricPosition DbData::getMetricPos(uint32_t id) const {
    shared_lock<shared_mutex> lk{m_mposMut};
    if (id >= m_metricPos.size())
        return {};
    return m_metricPos[id];
}

//===========================================================================
void DbData::setMetricPos(uint32_t id, const MetricPosition & mi) {
    shared_lock<shared_mutex> lk{m_mposMut};
    assert(id < m_metricPos.size());
    m_metricPos[id] = mi;
}

//===========================================================================
void DbData::onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn)
{}

//===========================================================================
void DbData::onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn)
{}

//===========================================================================
void DbData::onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn)
{}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void DbData::metricDestructPage (DbTxn & txn, uint32_t pgno) {
    auto mp = txn.viewPage<MetricPage>(pgno);
    radixDestruct(txn, mp->hdr);

    unique_lock<shared_mutex> lk{m_mposMut};
    m_metricPos[mp->hdr.id] = {};
    m_numMetrics -= 1;
    s_perfCount -= 1;
}

//===========================================================================
bool DbData::loadMetrics (
    DbTxn & txn,
    IDbDataNotify * notify,
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
        if (notify) {
            DbSeriesInfo info;
            info.id = mp->hdr.id;
            info.name = mp->name;
            info.type = mp->sampleType;
            info.last = info.first + mp->retention;
            info.interval = mp->interval;
            if (!notify->onDbSeriesStart(info))
                return false;
        }
        if (appStopping())
            return false;

        if (m_metricPos.size() <= mp->hdr.id)
            m_metricPos.resize(mp->hdr.id + 1);
        auto & mi = m_metricPos[mp->hdr.id];
        mi.infoPage = mp->hdr.pgno;
        mi.interval = mp->interval;
        mi.lastPage = mp->lastPage;
        mi.sampleType = mp->sampleType;

        s_perfCount += 1;
        m_numMetrics += 1;
        return true;
    }

    return false;
}

//===========================================================================
void DbData::insertMetric(DbTxn & txn, uint32_t id, string_view name) {
    assert(!name.empty());
    auto nameLen = metricNameSize(m_pageSize);
    if (name.size() >= nameLen)
        name = name.substr(0, nameLen - 1);

    // set info page
    auto pgno = allocPgno(txn);
    txn.logMetricInit(
        pgno,
        id,
        name,
        Clock::now(),
        kDefaultSampleType,
        kDefaultRetention,
        kDefaultInterval
    );

    // update index
    {
        scoped_lock<mutex> lk{m_mndxMut};
        bool inserted [[maybe_unused]] = radixInsert(
            txn,
            kMetricIndexPageNum,
            id,
            pgno
        );
        assert(inserted);
        s_perfCount += 1;
    }

    auto mp = txn.viewPage<MetricPage>(pgno);
    MetricPosition mi = {};
    mi.infoPage = mp->hdr.pgno;
    mi.interval = mp->interval;
    mi.sampleType = mp->sampleType;

    shared_lock<shared_mutex> lk{m_mposMut};
    if (id >= m_metricPos.size()) {
        lk.unlock();
        {
            unique_lock<shared_mutex> lk{m_mposMut};
            if (id >= m_metricPos.size())
                m_metricPos.resize(id + 1);
        }
        lk.lock();
    }

    assert(!m_metricPos[id].infoPage);
    m_metricPos[id] = mi;
    m_numMetrics += 1;
}

//===========================================================================
void DbData::onLogApplyMetricInit(
    void * ptr,
    uint32_t id,
    string_view name,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    if (mp->hdr.type == kPageTypeFree) {
        memset((char *) mp + sizeof(mp->hdr), 0, m_pageSize - sizeof(mp->hdr));
    } else {
        assert(!mp->hdr.type);
    }
    mp->hdr.type = mp->s_pageType;
    mp->hdr.id = id;
    mp->creation = creation;
    mp->sampleType = sampleType;
    mp->retention = retention;
    mp->interval = interval;
    auto count = name.copy(mp->name, metricNameSize(m_pageSize) - 1);
    auto rd = radixData(mp);
    memset(mp->name + count, 0, (char *) rd - mp->name - count);
    rd->height = 0;
    rd->numPages = entriesPerMetricPage();
}

//===========================================================================
bool DbData::eraseMetric(string * name, DbTxn & txn, uint32_t id) {
    auto mi = getMetricPos(id);
    if (mi.infoPage) {
        *name = txn.viewPage<MetricPage>(mi.infoPage)->name;
        auto rp = txn.viewPage<RadixPage>(kMetricIndexPageNum);
        scoped_lock<mutex> lk{m_mndxMut};
        radixErase(txn, rp->hdr, id, id + 1);
        return true;
    }
    return false;
}

//===========================================================================
void DbData::updateMetric(
    DbTxn & txn,
    uint32_t id,
    const DbMetricInfo & from
) {
    assert(from.name.empty());
    // TODO: validate interval, retention, and sample type

    auto mi = getMetricPos(id);
    if (!mi.infoPage)
        return;
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    DbMetricInfo info = {};
    info.retention = from.retention.count() ? from.retention : mp->retention;
    info.interval = from.interval.count() ? from.interval : mp->interval;
    info.type = from.type ? from.type : mp->sampleType;
    info.creation = from.creation ? from.creation : mp->creation;
    if (mp->retention == info.retention
        && mp->interval == info.interval
        && mp->sampleType == info.type
        && mp->creation == info.creation
    ) {
        return;
    }

    // Remove all existing samples
    radixDestruct(txn, mp->hdr);
    txn.logMetricUpdate(
        mi.infoPage,
        info.creation,
        info.type,
        info.retention,
        info.interval
    );

    mi.interval = info.interval;
    mi.sampleType = info.type;
    mi.lastPage = 0;
    mi.pageFirstTime = {};
    mi.pageLastSample = 0;
    shared_lock<shared_mutex> lk{m_mposMut};
    m_metricPos[id] = mi;
}

//===========================================================================
void DbData::getMetricInfo(
    IDbDataNotify * notify,
    const DbTxn & txn,
    uint32_t id
) {
    auto mi = loadMetricPos(txn, id);
    if (!mi.infoPage)
        return noSamples(notify, id, {}, kSampleTypeInvalid, {}, {});

    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    DbSeriesInfoEx info;
    info.id = id;
    info.name = mp->name;
    info.type = mp->sampleType;
    if (!mi.pageFirstTime) {
        info.last = info.first + mp->retention;
    } else {
        info.last = mi.pageFirstTime + mi.interval * mi.pageLastSample;
        info.first = info.last - mp->retention;
    }
    info.interval = mp->interval;
    info.retention = mp->retention;
    info.creation = mp->creation;
    if (notify->onDbSeriesStart(info))
        notify->onDbSeriesEnd(id);
}

//===========================================================================
void DbData::onLogApplyMetricUpdate(
    void * ptr,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->s_pageType);
    mp->creation = creation;
    mp->sampleType = sampleType;
    mp->retention = retention;
    mp->interval = interval;
    mp->lastPage = 0;
    mp->lastPagePos = 0;
    mp->lastPageFirstTime = {};
    auto rd = radixData(mp);
    rd->height = 0;
    memset(rd->pages, 0, rd->numPages * sizeof(*rd->pages));
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
size_t DbData::samplesPerPage(DbSampleType type) const {
    return ::samplesPerPage(type, m_pageSize);
}

//===========================================================================
static double getSample(const float * out) {
    return *out;
}

//===========================================================================
static double getSample(const double * out) {
    return *out;
}

//===========================================================================
template<typename T, typename = enable_if_t<is_integral_v<T>>>
static double getSample(const T * out) {
    const auto maxval = numeric_limits<T>::max();
    const auto minval = -maxval;
    T ival = *out;
    if (ival == minval - 1)
        return NAN;
    return ival;
}

//===========================================================================
static double getSample(const DbData::SamplePage * sp, size_t pos) {
    switch (sp->sampleType) {
    case kSampleTypeFloat32:
        return getSample(sp->samples.f32 + pos);
    case kSampleTypeFloat64:
        return getSample(sp->samples.f64 + pos);
    case kSampleTypeInt8:
        return getSample(sp->samples.i8 + pos);
    case kSampleTypeInt16:
        return getSample(sp->samples.i16 + pos);
    case kSampleTypeInt32:
        return getSample(sp->samples.i32 + pos);
    default:
        assert(!"Unknown sample type");
        return NAN;
    }
}

//===========================================================================
DbData::MetricPosition DbData::loadMetricPos(const DbTxn & txn, uint32_t id) {
    auto mi = getMetricPos(id);

    // Update metric info from sample page if it has no page data.
    if (mi.infoPage && mi.lastPage && !mi.pageFirstTime) {
        auto sp = txn.viewPage<SamplePage>(mi.lastPage);
        mi.pageFirstTime = sp->pageFirstTime;
        mi.pageLastSample = sp->pageLastSample;
        setMetricPos(id, mi);
    }
    return mi;
}

//===========================================================================
DbData::MetricPosition DbData::loadMetricPos(
    DbTxn & txn,
    uint32_t id,
    TimePoint time
) {
    auto mi = loadMetricPos(txn, id);
    if (!mi.infoPage || mi.lastPage)
        return mi;

    // metric has no sample pages
    // create empty page that covers the requested time

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto lastSample = (uint16_t) (id % samplesPerPage(mi.sampleType));
    auto pageTime = time - lastSample * mi.interval;
    auto spno = allocPgno(txn);
    txn.logSampleInit(spno, id, mi.sampleType, pageTime, lastSample);
    txn.logMetricUpdateSamples(mi.infoPage, 0, spno, pageTime, true);

    mi.lastPage = spno;
    mi.pageFirstTime = pageTime;
    mi.pageLastSample = lastSample;
    setMetricPos(id, mi);
    return mi;
}

//===========================================================================
void DbData::onLogApplyMetricClearSamples(void * ptr) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->s_pageType);
    mp->lastPage = 0;
    mp->lastPagePos = 0;
    mp->lastPageFirstTime = {};
    auto rd = radixData(mp);
    rd->height = 0;
    memset(rd->pages, 0, rd->numPages * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onLogApplyMetricUpdateSamples(
    void * ptr,
    size_t pos,
    uint32_t refPage,
    TimePoint refTime,
    bool updateIndex
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->s_pageType);
    mp->lastPage = refPage;
    mp->lastPagePos = (unsigned) pos;
    mp->lastPageFirstTime = refTime;
    if (updateIndex) {
        auto rd = radixData(mp);
        rd->pages[pos] = refPage;
    }
}

//===========================================================================
void DbData::updateSample(
    DbTxn & txn,
    uint32_t id,
    TimePoint time,
    double value
) {
    assert(time);

    // ensure all info about the last page is loaded, the expectation is that
    // almost all updates are to the last page.
    auto mi = loadMetricPos(txn, id, time);
    if (!mi.infoPage)
        return;

    // round time down to metric's sampling interval
    time -= time.time_since_epoch() % mi.interval;

    auto spp = samplesPerPage(mi.sampleType);
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
                txn.logSampleInit(
                    spno,
                    id,
                    mi.sampleType,
                    pageTime,
                    (uint16_t) spp - 1
                );
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
        auto ref = getSample(sp, ent);
        if (ref == value) {
            s_perfDup += 1;
        } else {
            if (isnan(ref)) {
                s_perfAdd += 1;
            } else {
                s_perfChange += 1;
            }
            txn.logSampleUpdateTxn(spno, ent, value, false);
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
            setMetricPos(id, mi);
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
        if (lastSample == mi.pageLastSample + 1) {
            txn.logSampleUpdateTxn(mi.lastPage, lastSample, value, true);
        } else {
            txn.logSampleUpdate(
                mi.lastPage,
                mi.pageLastSample + 1,
                lastSample,
                value,
                true
            );
        }
        mi.pageLastSample = lastSample;
        setMetricPos(id, mi);
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
        txn.logSampleInit(lastPage, id, mi.sampleType, endPageTime, 0);
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
    txn.logMetricUpdateSamples(mi.infoPage, last, lastPage, endPageTime, false);

    mi.lastPage = lastPage;
    mi.pageFirstTime = endPageTime;
    mi.pageLastSample = 0;
    setMetricPos(id, mi);

    // write sample to new last page
    updateSample(txn, id, time, value);
}

//===========================================================================
static void setSample(float * out, double value) {
    *out = (float) value;
}

//===========================================================================
static void setSample(double * out, double value) {
    *out = (double) value;
}

//===========================================================================
template<typename T, typename = enable_if_t<is_integral_v<T>>>
static void setSample(T * out, double value) {
    const auto maxval = numeric_limits<T>::max();
    const auto minval = -maxval;
    T ival = isnan(value) ? minval - 1
        : value < minval ? minval
        : value > maxval ? maxval
        : (T) value;
    *out = ival;
}

//===========================================================================
static void setSample(
    DbData::SamplePage * sp,
    size_t pos,
    double value
) {
    switch (sp->sampleType) {
    case kSampleTypeFloat32:
        return setSample(sp->samples.f32 + pos, value);
    case kSampleTypeFloat64:
        return setSample(sp->samples.f64 + pos, value);
    case kSampleTypeInt8:
        return setSample(sp->samples.i8 + pos, value);
    case kSampleTypeInt16:
        return setSample(sp->samples.i16 + pos, value);
    case kSampleTypeInt32:
        return setSample(sp->samples.i32 + pos, value);
    default:
        assert(!"unknown sample type");
    }
}

//===========================================================================
static void clearSamples(
    DbData::SamplePage * sp,
    size_t firstPos,
    size_t lastPos
) {
    switch (sp->sampleType) {
    case kSampleTypeFloat32:
        for (auto i = firstPos; i < lastPos; ++i)
            sp->samples.f32[i] = NAN;
        break;
    case kSampleTypeFloat64:
        for (auto i = firstPos; i < lastPos; ++i)
            sp->samples.f64[i] = NAN;
        break;
    case kSampleTypeInt8:
        memset(
            sp->samples.i8 + firstPos,
            -128,
            (lastPos - firstPos) * sizeof(int8_t)
        );
        break;
    case kSampleTypeInt16:
        for (auto i = firstPos; i < lastPos; ++i)
            sp->samples.i16[i] = numeric_limits<int16_t>::min();
        break;
    case kSampleTypeInt32:
        for (auto i = firstPos; i < lastPos; ++i)
            sp->samples.i32[i] = numeric_limits<int32_t>::min();
        break;
    default:
        assert(!"unknown sample type");
        break;
    }
}

//===========================================================================
void DbData::onLogApplySampleInit(
    void * ptr,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample
) {
    auto sp = static_cast<SamplePage *>(ptr);
    if (sp->hdr.type == kPageTypeFree) {
        memset((char *) sp + sizeof(sp->hdr), 0, m_pageSize - sizeof(sp->hdr));
    } else {
        assert(!sp->hdr.type);
    }
    sp->hdr.type = sp->s_pageType;
    sp->hdr.id = id;
    sp->sampleType = sampleType;
    sp->pageLastSample = (uint16_t) lastSample;
    sp->pageFirstTime = pageTime;
    auto vpp = samplesPerPage(sampleType);
    clearSamples(sp, 0, vpp);
}

//===========================================================================
void DbData::onLogApplySampleUpdate(
    void * ptr,
    size_t firstPos,
    size_t lastPos,
    double value,
    bool updateLast
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->s_pageType);
    clearSamples(sp, firstPos, lastPos);
    if (!isnan(value))
        setSample(sp, lastPos, value);
    if (updateLast)
        sp->pageLastSample = (uint16_t) lastPos;
}

//===========================================================================
void DbData::onLogApplySampleUpdateTime(void * ptr, Dim::TimePoint pageTime) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->s_pageType);
    sp->pageFirstTime = pageTime;
    sp->pageLastSample = 0;
    setSample(sp, 0, NAN);
}

//===========================================================================
void DbData::getSamples(
    DbTxn & txn,
    IDbDataNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last,
    unsigned presamples
) {
    auto mi = loadMetricPos(txn, id);
    if (!mi.infoPage)
        return noSamples(notify, id, {}, kSampleTypeInvalid, {}, {});
    auto mp = txn.viewPage<MetricPage>(mi.infoPage);
    auto name = string_view(mp->name);
    auto stype = mp->sampleType;

    // round time to metric's sampling interval
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    // expand range to include presamples
    first -= presamples * mi.interval;

    if (!mi.lastPage)
        return noSamples(notify, id, name, stype, last, mi.interval);

    auto lastSampleTime = mi.pageFirstTime + mi.pageLastSample * mi.interval;
    auto firstSampleTime = lastSampleTime - mp->retention + mi.interval;
    if (first < firstSampleTime)
        first = firstSampleTime;
    if (last > lastSampleTime)
        last = lastSampleTime;
    if (first >= last)
        return noSamples(notify, id, name, stype, last, mi.interval);

    auto vpp = samplesPerPage(mi.sampleType);
    auto pageInterval = vpp * mi.interval;
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / vpp + 1;

    uint32_t spno;
    unsigned sppos;
    if (first >= mi.pageFirstTime) {
        sppos = mp->lastPagePos;
        spno = mi.lastPage;
    } else {
        auto off = (mi.pageFirstTime - first - mi.interval) / pageInterval + 1;
        sppos = (uint32_t) (mp->lastPagePos + numPages - off) % numPages;
        if (!radixFind(txn, &spno, mi.infoPage, sppos))
            spno = 0;
    }

    DbSeriesInfo dsi;
    dsi.id = id;
    dsi.name = name;
    dsi.type = stype;
    dsi.interval = mi.interval;
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
                auto value = getSample(sp, vpos);
                if (!isnan(value)) {
                    if (!count++) {
                        dsi.first = first;
                        dsi.last = last + mi.interval;
                        if (!notify->onDbSeriesStart(dsi))
                            return;
                    }
                    if (!notify->onDbSample(id, first, value))
                        return;
                }
            }
        }
        if (first > last)
            break;

        // advance to next page
        sppos = (sppos + 1) % numPages;
        radixFind(txn, &spno, mi.infoPage, sppos);
    }
    if (!count) {
        return noSamples(notify, id, name, stype, last, mi.interval);
    } else {
        notify->onDbSeriesEnd(id);
    }
}


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
DbData::RadixData * DbData::radixData(MetricPage * mp) const {
    auto ents = entriesPerMetricPage();
    auto off = offsetof(RadixData, pages) + ents * sizeof(uint32_t);
    auto ptr = (char *) mp + m_pageSize - off;
    return reinterpret_cast<DbData::RadixData *>(ptr);
}

//===========================================================================
DbData::RadixData * DbData::radixData(DbPageHeader * hdr) const {
    if (hdr->type == kPageTypeMetric) {
        auto mp = reinterpret_cast<DbData::MetricPage *>(hdr);
        return radixData(mp);
    } else {
        assert(hdr->type == kPageTypeRadix);
        return &reinterpret_cast<DbData::RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
const DbData::RadixData * DbData::radixData(const DbPageHeader * hdr) const {
    return radixData(const_cast<DbPageHeader *>(hdr));
}

//===========================================================================
uint16_t DbData::entriesPerMetricPage() const {
    auto off = offsetof(MetricPage, name) + metricNameSize(m_pageSize)
        + offsetof(RadixData, pages);
    return (uint16_t) (m_pageSize - off) / sizeof(uint32_t);
}

//===========================================================================
uint16_t DbData::entriesPerRadixPage() const {
    auto off = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    return (uint16_t) (m_pageSize - off) / sizeof(uint32_t);
}

//===========================================================================
size_t DbData::radixPageEntries(
    int * out,
    size_t outLen,
    DbPageType rootType,
    uint16_t height,
    size_t pos
) {
    int * base = out;
    size_t pents = entriesPerRadixPage();
    size_t rents;
    if (rootType == kPageTypeMetric) {
        rents = entriesPerMetricPage();
    } else {
        assert(rootType == kPageTypeRadix);
        rents = pents;
    }

    for (;;) {
        *out++ = (int) (pos % pents);
        if (pos < rents)
            break;
        pos /= pents;
    }

    // always return at least "height" entries
    for (int * end = base + height + 1; out < end; ++out)
        *out = 0;
    reverse(base, out);
    return out - base;
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
        txn.logRadixErase(hdr->pgno, rpos, lastPagePos);
        firstPos = firstPos + lastPagePos - rpos;
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
    auto id = hdr->id;
    auto rd = radixData(hdr);

    int digits[10];
    size_t count = radixPageEntries(
        digits,
        size(digits),
        hdr->type,
        rd->height,
        pos
    );
    count -= 1;
    while (rd->height < count) {
        auto pgno = allocPgno(txn);
        txn.logRadixInit(
            pgno,
            id,
            rd->height,
            rd->pages,
            rd->pages + rd->numPages
        );
        txn.logRadixPromote(root, pgno);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        auto pgno = rd->pages[pos];
        if (!pgno) {
            pgno = allocPgno(txn);
            txn.logRadixInit(
                pgno,
                id,
                rd->height - 1,
                nullptr,
                nullptr
            );
            txn.logRadixUpdate(hdr->pgno, pos, pgno);
        }
        hdr = txn.viewPage<DbPageHeader>(pgno);
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
void DbData::onLogApplyRadixInit(
    void * ptr,
    uint32_t id,
    uint16_t height,
    const uint32_t * firstPgno,
    const uint32_t * lastPgno
) {
    auto rp = static_cast<RadixPage *>(ptr);
    if (rp->hdr.type == kPageTypeFree) {
        memset((char *) rp + sizeof(rp->hdr), 0, m_pageSize - sizeof(rp->hdr));
    } else {
        assert(!rp->hdr.type);
    }
    rp->hdr.type = rp->s_pageType;
    rp->hdr.id = id;
    rp->rd.height = height;
    rp->rd.numPages = entriesPerRadixPage();
    if (auto count = lastPgno - firstPgno) {
        assert(count <= rp->rd.numPages);
        memcpy(rp->rd.pages, firstPgno, count * sizeof(*firstPgno));
    }
}

//===========================================================================
void DbData::onLogApplyRadixErase(void * ptr, size_t firstPos, size_t lastPos) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == kPageTypeMetric || hdr->type == kPageTypeRadix);
    auto rd = radixData(hdr);
    assert(firstPos < lastPos);
    assert(lastPos <= rd->numPages);
    memset(rd->pages + firstPos, 0, (lastPos - firstPos) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onLogApplyRadixPromote(void * ptr, uint32_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == kPageTypeMetric || hdr->type == kPageTypeRadix);
    auto rd = radixData(hdr);
    rd->height += 1;
    rd->pages[0] = refPage;
    memset(rd->pages + 1, 0, (rd->numPages - 1) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onLogApplyRadixUpdate(void * ptr, size_t pos, uint32_t refPage) {
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

    int digits[10];
    size_t count = radixPageEntries(
        digits,
        size(digits),
        (*hdr)->type,
        (*rd)->height,
        pos
    );
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
    logMsgDebug() << "Free pages: " << m_freePages;

    // validate that pages in free list are in fact free
    uint32_t blank = 0;
    for (auto && pgno : m_freePages) {
        if (pgno >= m_numPages)
            break;
        auto fp = txn.viewPage<DbPageHeader>(pgno);
        if (!fp || fp->type && fp->type != kPageTypeFree) {
            logMsgError() << "Bad free page #" << pgno;
            return false;
        }
        if (fp->type) {
            if (blank) {
                logMsgError() << "Blank data page #" << pgno;
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
    scoped_lock<recursive_mutex> lk{m_pageMut};

    if (m_freePages.empty()) {
        auto [segPage, segPos] = segmentPage((uint32_t) m_numPages, m_pageSize);
        assert(segPage == m_numPages && !segPos);
        (void) segPos;
        m_numPages += 1;
        txn.growToFit(segPage);
        auto pps = pagesPerSegment(m_pageSize);
        m_freePages.insert(segPage + 1, segPage + pps - 1);
    }
    auto pgno = m_freePages.pop_front();

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
    scoped_lock<recursive_mutex> lk{m_pageMut};

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
void DbData::onLogApplyPageFree(void * ptr) {
    auto fp = static_cast<FreePage *>(ptr);
    assert(fp->hdr.type && fp->hdr.type != kPageTypeFree);
    fp->hdr.type = kPageTypeFree;
}

//===========================================================================
void DbData::onLogApplyZeroInit(void * ptr) {
    auto zp = static_cast<ZeroPage *>(ptr);
    assert(!zp->hdr.type);
    zp->hdr.type = zp->s_pageType;
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
void DbData::onLogApplySegmentUpdate(
    void * ptr,
    uint32_t refPage,
    bool free
) {
    auto sp = static_cast<SegmentPage *>(ptr);
    auto bits = segmentBitView(sp, m_pageSize);
    if (!sp->hdr.type) {
        sp->hdr.type = sp->s_pageType;
        sp->hdr.id = 0;
        if constexpr (DIMAPP_LIB_BUILD_DEBUG) {
            auto [segPage, segPos] = segmentPage(sp->hdr.pgno, m_pageSize);
            assert(segPage == sp->hdr.pgno && !segPos);
        }
        bits.set();
        bits.reset(0);
    }
    assert(sp->hdr.type == kPageTypeZero || sp->hdr.type == kPageTypeSegment);
    auto [segPage, segPos] = segmentPage(refPage, m_pageSize);
    assert(sp->hdr.pgno == segPage);
    ignore = segPage;
    assert(bits[segPos] != free);
    bits.set(segPos, free);
}
