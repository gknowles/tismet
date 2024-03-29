// Copyright Glen Knowles 2017 - 2023.
// Distributed under the Boost Software License, Version 1.0.
//
// dbmetricdata.cpp - tismet db
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
constexpr Duration kDefaultRetention = 7 * 24h;
constexpr Duration kDefaultInterval = 1min;
static_assert(kDefaultRetention >= kDefaultInterval);

const unsigned kMaxMetricNameLen = 128;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());


/****************************************************************************
*
*   Private
*
***/

struct DbData::MetricPage {
    static const auto kPageType = DbPageType::kMetric;
    DbPageHeader hdr;
    TimePoint creation;
    Duration interval;
    Duration retention;
    TimePoint lastPageFirstTime;
    uint16_t lastPageSample;
    uint16_t reserved;
    unsigned lastPagePos;
    DbSampleType sampleType;

    // EXTENDS BEYOND END OF STRUCT
    char name[1];

    // RadixData object immediately follows name
};
static_assert(sizeof(DbData::MetricPage) <= kMinPageSize);

struct DbData::SamplePage {
    static const auto kPageType = DbPageType::kSample;
    DbPageHeader hdr;

    // time of first sample on page
    TimePoint pageFirstTime;

    // Position of last sample, samples that come after this position on the
    // page are either in the not yet populated future or (because it's a giant
    // discontinuous ring buffer) in the distant past.
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

static auto & s_perfAncient = uperf("db.samples ignored (old)");
static auto & s_perfDup = uperf("db.samples ignored (dup)");
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
constexpr pair<pgno_t, size_t> segmentPage(pgno_t pgno, size_t pageSize) {
    auto pps = pagesPerSegment(pageSize);
    auto segPage = pgno / pps * pps;
    auto segPos = pgno % pps;
    return {(pgno_t) segPage, segPos};
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
DbData::MetricPosition DbData::getMetricPos(uint32_t id) const {
    shared_lock lk{m_mposMut};
    if (id >= m_metricPos.size())
        return {};
    return m_metricPos[id];
}

//===========================================================================
void DbData::setMetricPos(uint32_t id, const MetricPosition & mi) {
    shared_lock lk{m_mposMut};
    assert(id < m_metricPos.size());
    m_metricPos[id] = mi;
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void DbData::metricClearCounters() {
    s_perfCount -= m_numMetrics;
}

//===========================================================================
// static
size_t DbData::metricNameSize(size_t pageSize) {
    assert(pageSize > sizeof(DbData::MetricPage) + sizeof(DbData::RadixData));
    auto count = pageSize - sizeof(DbData::MetricPage)
        - sizeof(DbData::RadixData);
    if (count > kMaxMetricNameLen)
        count = kMaxMetricNameLen;
    return count;
}

//===========================================================================
void DbData::metricDestructPage(DbTxn & txn, pgno_t pgno) {
    auto mp = txn.pin<MetricPage>(pgno);
    radixDestruct(txn, mp->hdr);

    unique_lock lk{m_mposMut};
    m_metricPos[mp->hdr.id] = {};
    m_numMetrics -= 1;
    s_perfCount -= 1;
}

//===========================================================================
bool DbData::loadMetric(DbTxn & txn, IDbDataNotify * notify, pgno_t pgno) {
    auto mp = txn.pin<MetricPage>(pgno);
    if (mp->hdr.type != DbPageType::kMetric) {
        logMsgError() << "Bad metric page #" << pgno << ", type "
            << (unsigned) mp->hdr.type;
        return false;
    }
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
    pgno_t lastPage;
    if (!radixFind(txn, &lastPage, pgno, mp->lastPagePos)
        && !empty(mp->lastPageFirstTime)
    ) {
        return false;
    }
    if (appStopping())
        return false;

    if (m_metricPos.size() <= mp->hdr.id)
        m_metricPos.resize(mp->hdr.id + 1);
    auto & mi = m_metricPos[mp->hdr.id];
    mi.infoPage = pgno;
    mi.interval = mp->interval;
    mi.lastPage = lastPage;
    mi.sampleType = mp->sampleType;

    s_perfCount += 1;
    m_numMetrics += 1;
    return true;
}

//===========================================================================
bool DbData::loadMetrics(DbTxn & txn, IDbDataNotify * notify) {
    return radixVisit(
        txn,
        m_metricRoot,
        [notify, this](DbTxn & txn, auto index, auto pgno) {
            return loadMetric(txn, notify, pgno);
        }
    );
}

//===========================================================================
void DbData::insertMetric(DbTxn & txn, uint32_t id, string_view name) {
    assert(!name.empty());
    auto nameLen = metricNameSize(m_pageSize);
    if (name.size() >= nameLen)
        name = name.substr(0, nameLen - 1);

    // set info page
    auto pgno = allocPgno(txn);
    txn.walMetricInit(
        pgno,
        id,
        name,
        timeNow(),
        kDefaultSampleType,
        kDefaultRetention,
        kDefaultInterval
    );

    // update id index
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        radixInsert(
            txn,
            m_metricRoot,
            id,
            pgno
        );
        s_perfCount += 1;
    }

if constexpr (1) {
    // update name index
    vector<shared_ptr<DbRootVersion>> roots = { txn.roots().name };
    vector<string> keys = { trieKey(name, id) };
    trieInsert(txn, roots, keys);
}

    // update in memory references
    auto mp = txn.pin<MetricPage>(pgno);
    MetricPosition mi = {};
    mi.infoPage = mp->hdr.pgno;
    mi.interval = mp->interval;
    mi.sampleType = mp->sampleType;

    shared_lock lk{m_mposMut};
    if (id >= m_metricPos.size()) {
        lk.unlock();
        {
            unique_lock lk{m_mposMut};
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
void DbData::onWalApplyMetricInit(
    void * ptr,
    uint32_t id,
    string_view name,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    if (mp->hdr.type == DbPageType::kFree) {
        memset((char *) mp + sizeof(mp->hdr), 0, m_pageSize - sizeof(mp->hdr));
    } else {
        assert(mp->hdr.type == DbPageType::kInvalid);
    }
    mp->hdr.type = mp->kPageType;
    mp->hdr.id = id;
    mp->creation = creation;
    mp->sampleType = sampleType;
    mp->retention = retention;
    mp->interval = interval;
    auto count = name.copy(mp->name, metricNameSize(m_pageSize) - 1);
    auto rd = radixData(mp, m_pageSize);
    memset(mp->name + count, 0, (char *) rd - mp->name - count);
    rd->height = 0;
    rd->numPages = entriesPerMetricPage(m_pageSize);
}

//===========================================================================
bool DbData::eraseMetric(string * name, DbTxn & txn, uint32_t id) {
    auto mi = getMetricPos(id);
    if (!mi.infoPage)
        return false;

    *name = txn.pin<MetricPage>(mi.infoPage)->name;

    // update id index
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        radixErase(txn, m_metricRoot, id, id + 1);
    }

if constexpr (1) {
    // update name index
    vector<shared_ptr<DbRootVersion>> roots = { txn.roots().name };
    vector<string> keys = { trieKey(*name, id) };
    trieErase(txn, roots, keys);
}

    return true;
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
    auto mp = txn.pin<MetricPage>(mi.infoPage);
    DbMetricInfo info = {};
    info.retention = from.retention.count() ? from.retention : mp->retention;
    info.interval = from.interval.count() ? from.interval : mp->interval;
    info.type = from.type ? from.type : mp->sampleType;
    info.creation = !empty(from.creation) ? from.creation : mp->creation;
    if (mp->retention == info.retention
        && mp->interval == info.interval
        && mp->sampleType == info.type
        && mp->creation == info.creation
    ) {
        return;
    }

    // Remove all existing samples
    radixDestruct(txn, mp->hdr);
    txn.walMetricUpdate(
        mi.infoPage,
        info.creation,
        info.type,
        info.retention,
        info.interval
    );

    // Reset in memory references
    mi.interval = info.interval;
    mi.sampleType = info.type;
    mi.lastPage = {};
    mi.pageFirstTime = {};
    mi.pageLastSample = 0;
    shared_lock lk{m_mposMut};
    m_metricPos[id] = mi;
}

//===========================================================================
void DbData::getMetricInfo(IDbDataNotify * notify, DbTxn & txn, uint32_t id) {
    auto mi = loadMetricPos(txn, id);
    if (!mi.infoPage)
        return noSamples(notify, id, {}, kSampleTypeInvalid, {}, {});

    auto mp = txn.pin<MetricPage>(mi.infoPage);
    DbSeriesInfoEx info;
    info.id = id;
    info.name = mp->name;
    info.type = mp->sampleType;
    if (empty(mi.pageFirstTime)) {
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
void DbData::onWalApplyMetricUpdate(
    void * ptr,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    mp->creation = creation;
    mp->sampleType = sampleType;
    mp->retention = retention;
    mp->interval = interval;
    mp->lastPagePos = 0;
    mp->lastPageFirstTime = {};
    mp->lastPageSample = 0;
    auto rd = radixData(mp, m_pageSize);
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
template<typename T>
static double getSample(const T * out) {
    if constexpr (is_same_v<T, pgno_t>) {
        if (*out <= kMaxPageNum)
            return NAN;
        return (double) *out - (kMaxPageNum + kMaxPageNum / 2);
    } else if constexpr (is_floating_point_v<T>) {
        return *out;
    } else if constexpr (is_integral_v<T>) {
        constexpr auto maxval = numeric_limits<T>::max();
        constexpr auto minval = -maxval;
        T ival = *out;
        if (ival == minval - 1)
            return NAN;
        return ival;
    } else {
        assert(!"Sample type must be numeric");
        return NAN;
    }
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
DbData::MetricPosition DbData::loadMetricPos(DbTxn & txn, uint32_t id) {
    auto mi = getMetricPos(id);

    // Update metric info from sample page if it has no page data.
    if (mi.infoPage && mi.lastPage && empty(mi.pageFirstTime)) {
        if (mi.lastPage > kMaxPageNum) {
            auto mp = txn.pin<MetricPage>(mi.infoPage);
            mi.pageFirstTime = mp->lastPageFirstTime;
            mi.pageLastSample = mp->lastPageSample;
        } else {
            auto sp = txn.pin<SamplePage>(mi.lastPage);
            mi.pageFirstTime = sp->pageFirstTime;
            mi.pageLastSample = sp->pageLastSample;
        }
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

    // Metric has no sample pages create empty page that covers the requested
    // time.

    // Round time down to metric's sampling interval.
    time -= time.time_since_epoch() % mi.interval;

    auto lastSample = (uint16_t) (id % samplesPerPage(mi.sampleType));
    auto pageTime = time - lastSample * mi.interval;
    auto spno = allocPgno(txn);
    txn.walSampleInit(spno, id, mi.sampleType, pageTime, lastSample);
    [[maybe_unused]] auto mp = txn.pin<MetricPage>(mi.infoPage);
    txn.walMetricUpdateSamples(mi.infoPage, 0, pageTime, (size_t) -1, spno);

    mi.lastPage = spno;
    mi.pageFirstTime = pageTime;
    mi.pageLastSample = lastSample;
    setMetricPos(id, mi);
    return mi;
}

//===========================================================================
void DbData::onWalApplyMetricClearSamples(void * ptr) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    mp->lastPagePos = 0;
    mp->lastPageFirstTime = {};
    mp->lastPageSample = 0;
    auto rd = radixData(mp, m_pageSize);
    rd->height = 0;
    memset(rd->pages, 0, rd->numPages * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onWalApplyMetricUpdateSamples(
    void * ptr,
    size_t pos,
    TimePoint refTime,
    size_t refSample,
    pgno_t refPage
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    if (!empty(refTime)) {
        assert(pos != -1);
        mp->lastPagePos = (unsigned) pos;
        mp->lastPageFirstTime = refTime;
    }
    mp->lastPageSample = (uint16_t) refSample;
    if (refPage) {
        auto rd = radixData(mp, m_pageSize);
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
    assert(!empty(time));
    const auto kInvalidPos = (size_t) -1;

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

    if (time <= lastSampleTime) {
        // updating historical sample
        auto spno = mi.lastPage;
        auto sppos = kInvalidPos;
        auto pageTime = mi.pageFirstTime;
        auto ent = kInvalidPos;
        if (time >= mi.pageFirstTime) {
            // updating sample on tip page
            assert(spno);
        } else {
            // updating sample on old page
            auto mp = txn.pin<MetricPage>(mi.infoPage);
            auto firstSampleTime = lastSampleTime - mp->retention + mi.interval;
            if (time < firstSampleTime) {
                // sample older than retention, ignore it
                s_perfAncient += 1;
                return;
            }

            auto numSamples = mp->retention / mi.interval;
            auto numPages = (numSamples - 1) / spp + 1;
            auto poff = (mi.pageFirstTime - time + pageInterval - mi.interval)
                / pageInterval;
            pageTime = mi.pageFirstTime - poff * pageInterval;
            sppos = (mp->lastPagePos + numPages - poff) % numPages;
            if (sppos == mp->lastPagePos) {
                // Still on the tip page of the ring buffer, but in the old
                // samples section.
                sppos = kInvalidPos;
                ent = (time - pageTime) / mi.interval;
            } else {
                radixFind(txn, &spno, mi.infoPage, sppos);
                if (!spno) {
                    spno = sampleMakePhysical(
                        txn,
                        id,
                        mi,
                        sppos,
                        pageTime,
                        spp - 1
                    );
                }
            }
        }
        if (spno > kMaxPageNum) {
            auto fill = getSample(&spno);
            if (fill == value) {
                s_perfDup += 1;
                return;
            }
            if (time >= mi.pageFirstTime // new samples section on tip page
                || ent != kInvalidPos    // old section on tip page
            ) {
                // converting last page
                assert(sppos == kInvalidPos);
                auto mp = txn.pin<MetricPage>(mi.infoPage);
                spno = sampleMakePhysical(
                    txn,
                    id,
                    mi,
                    mp->lastPagePos,
                    mp->lastPageFirstTime,
                    mp->lastPageSample,
                    spno
                );
                // update references to last page
                mi.lastPage = spno;
                setMetricPos(id, mi);
            } else {
                // converting old page
                spno = sampleMakePhysical(
                    txn,
                    id,
                    mi,
                    sppos,
                    pageTime,
                    spp - 1,
                    spno
                );
            }
        }
        auto sp = txn.pin<SamplePage>(spno);
        if (ent == kInvalidPos) {
            assert(time >= sp->pageFirstTime);
            ent = (time - sp->pageFirstTime) / mi.interval;
        }
        assert(ent < (unsigned) spp);
        auto ref = getSample(sp, ent);
        if (ref == value) {
            s_perfDup += 1;
        } else {
            if (isnan(ref)) {
                if (isnan(value)) {
                    s_perfDup += 1;
                    return;
                }
                s_perfAdd += 1;
            } else {
                s_perfChange += 1;
            }
            txn.walSampleUpdateTxn(spno, ent, value, false);
            if (sampleTryMakeVirtual(txn, mi, spno))
                setMetricPos(id, mi);
        }
        return;
    }

    //-----------------------------------------------------------------------
    // After last known sample

    // If past the end of the page, check if it's also past the retention of all
    // pages.
    if (time >= endPageTime) {
        auto mp = txn.pin<MetricPage>(mi.infoPage);
        // further in the future than the retention period? remove all samples
        // and add as new initial sample.
        if (time >= lastSampleTime + mp->retention) {
            radixDestruct(txn, mp->hdr);
            txn.walMetricClearSamples(mi.infoPage);
            mi.lastPage = {};
            mi.pageFirstTime = {};
            mi.pageLastSample = 0;
            setMetricPos(id, mi);
            updateSample(txn, id, time, value);
            return;
        }
    }

    // update last page
    if (time < endPageTime) {
        auto ent = (uint16_t) ((time - mi.pageFirstTime) / mi.interval);
        s_perfAdd += 1;
        if (mi.lastPage > kMaxPageNum) {
            auto fill = getSample(&mi.lastPage);
            if (fill == value && ent == mi.pageLastSample + 1) {
                txn.walMetricUpdateSamplesTxn(mi.infoPage, ent);
                mi.pageLastSample = ent;
                setMetricPos(id, mi);
                return;
            }
            auto mp = txn.pin<MetricPage>(mi.infoPage);
            mi.lastPage = sampleMakePhysical(
                txn,
                id,
                mi,
                mp->lastPagePos,
                mi.pageFirstTime,
                mi.pageLastSample,
                mi.lastPage
            );
        }
        [[maybe_unused]] auto sp = txn.pin<SamplePage>(mi.lastPage);
        assert(mi.pageFirstTime == sp->pageFirstTime);
        assert(mi.pageLastSample == sp->pageLastSample);
        if (ent == mi.pageLastSample + 1) {
            txn.walSampleUpdateTxn(mi.lastPage, ent, value, true);
            mi.pageLastSample = ent;
            if (ent == spp - 1)
                sampleTryMakeVirtual(txn, mi, mi.lastPage);
        } else {
            txn.walSampleUpdate(
                mi.lastPage,
                mi.pageLastSample + 1,
                ent,
                value,
                true
            );
            mi.pageLastSample = ent;
        }
        setMetricPos(id, mi);
        return;
    }

    if (mi.lastPage <= kMaxPageNum) {
        [[maybe_unused]] auto sp = txn.pin<SamplePage>(mi.lastPage);
        txn.walSampleUpdate(mi.lastPage, mi.pageLastSample + 1, spp, NAN, true);
    } else {
        if (mi.pageLastSample + 1 < spp) {
            auto mp = txn.pin<MetricPage>(mi.infoPage);
            mi.lastPage = sampleMakePhysical(
                txn,
                id,
                mi,
                mp->lastPagePos,
                mi.pageFirstTime,
                mi.pageLastSample,
                mi.lastPage
            );
            txn.walSampleUpdate(
                mi.lastPage,
                mi.pageLastSample + 1,
                spp,
                NAN,
                true
            );
        }
    }
    mi.pageLastSample = (uint16_t) spp;

    //-----------------------------------------------------------------------
    // sample is after last page

    // delete pages between last page and the one the sample is on
    auto num = (time - endPageTime) / pageInterval;
    auto mp = txn.pin<MetricPage>(mi.infoPage);
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / spp + 1;
    auto first = (mp->lastPagePos + 1) % numPages;
    auto last = first + num;
    if (num) {
        endPageTime += num * pageInterval;
        if (last <= numPages) {
            radixErase(txn, mp->hdr.pgno, first, last);
        } else {
            last %= numPages;
            radixErase(txn, mp->hdr.pgno, first, numPages);
            radixErase(txn, mp->hdr.pgno, 0, last);
        }
    }

    // update reference to last sample page
    pgno_t lastPage;
    if (radixFind(txn, &lastPage, mi.infoPage, last)
        && lastPage <= kMaxPageNum
    ) {
        [[maybe_unused]] auto sp = txn.pin<SamplePage>(lastPage);
        txn.walSampleUpdateTime(lastPage, endPageTime);
    } else {
        lastPage = sampleMakePhysical(
            txn,
            id,
            mi,
            last,
            endPageTime,
            0,
            lastPage
        );
    }
    txn.walMetricUpdateSamples(
        mi.infoPage,
        last,
        endPageTime,
        0,
        {}
    );

    mi.lastPage = lastPage;
    mi.pageFirstTime = endPageTime;
    mi.pageLastSample = 0;
    setMetricPos(id, mi);

    // write sample to new last page
    updateSample(txn, id, time, value);
}

//===========================================================================
template<typename T>
static void setSample(T * out, double value) {
    if constexpr (is_same_v<T, pgno_t>) {
        auto oval = isnan(value) ? 0
            : value < kMinVirtualSample ? kMinVirtualSample
            : value > kMaxVirtualSample ? kMaxVirtualSample
            : (int) value + kMaxPageNum + kMaxPageNum / 2;
        *out = (T) oval;
    } else if constexpr (is_floating_point_v<T>) {
        *out = (T) value;
    } else if constexpr (is_integral_v<T>) {
        const auto maxval = numeric_limits<T>::max();
        const auto minval = -maxval;
        *out = isnan(value) ? minval - 1
            : value < minval ? minval
            : value > maxval ? maxval
            : (T) value;
    } else {
        assert(!"Sample type must be numeric");
        *out = NAN;
    }
}

//===========================================================================
static void setSample(DbData::SamplePage * sp, size_t pos, double value) {
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
template<typename T>
static void setSamples(T * out, size_t count, double value) {
    if (count) {
        setSample(out, value);
        for (auto i = 1; i < count; ++i)
            out[i] = *out;
    }
}

//===========================================================================
static void setSamples(
    DbData::SamplePage * sp,
    size_t firstPos,
    size_t lastPos,
    double value
) {
    switch (sp->sampleType) {
    case kSampleTypeFloat32:
        setSamples(sp->samples.f32 + firstPos, lastPos - firstPos, value);
        break;
    case kSampleTypeFloat64:
        setSamples(sp->samples.f64 + firstPos, lastPos - firstPos, value);
        break;
    case kSampleTypeInt8:
        setSamples(sp->samples.i8 + firstPos, lastPos - firstPos, value);
        break;
    case kSampleTypeInt16:
        setSamples(sp->samples.i16 + firstPos, lastPos - firstPos, value);
        break;
    case kSampleTypeInt32:
        setSamples(sp->samples.i32 + firstPos, lastPos - firstPos, value);
        break;
    default:
        assert(!"unknown sample type");
        break;
    }
}

//===========================================================================
pgno_t DbData::sampleMakePhysical(
    DbTxn & txn,
    uint32_t id,
    DbData::MetricPosition & mi,
    size_t sppos,
    TimePoint pageTime,
    size_t lastSample,
    pgno_t vpage
) {
    auto fill = (double) NAN;
    if (vpage) {
        fill = getSample(&vpage);
        assert(!isnan(fill));
    }
    auto spno = allocPgno(txn);
    txn.walSampleInit(
        spno,
        id,
        mi.sampleType,
        pageTime,
        lastSample,
        fill
    );
    radixSwapValue(txn, mi.infoPage, sppos, spno);
    return spno;
}

//===========================================================================
bool DbData::sampleTryMakeVirtual(
    DbTxn & txn,
    DbData::MetricPosition & mi,
    pgno_t spno
) {
    auto sp = txn.pin<SamplePage>(spno);
    auto value = getSample(sp, 0);
    if (isnan(value))
        return false;
    pgno_t vpage;
    setSample(&vpage, value);
    if (value != getSample(&vpage))
        return false;

    auto spp = samplesPerPage(mi.sampleType);
    for (auto i = 1; i < spp; ++i) {
        if (value != getSample(sp, i))
            return false;
    }

    auto mp = txn.pin<MetricPage>(mi.infoPage);
    if (spno == mi.lastPage) {
        auto sppos = mp->lastPagePos;
        auto pgno = radixSwapValue(txn, mi.infoPage, sppos, vpage);
        freePage(txn, pgno);
        txn.walMetricUpdateSamplesTxn(mi.infoPage, mi.pageLastSample);
        mi.lastPage = vpage;
    } else {
        auto pageInterval = spp * mi.interval;
        auto numSamples = mp->retention / mp->interval;
        auto numPages = (numSamples - 1) / spp + 1;
        auto sptime = sp->pageFirstTime;
        auto poff = (mi.pageFirstTime - sptime + pageInterval - mi.interval)
            / pageInterval;
        auto sppos = (mp->lastPagePos + numPages - poff) % numPages;
        auto pgno = radixSwapValue(txn, mi.infoPage, sppos, vpage);
        freePage(txn, pgno);
    }
    return true;
}

//===========================================================================
void DbData::onWalApplySampleInit(
    void * ptr,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample,
    double fill
) {
    auto sp = static_cast<SamplePage *>(ptr);
    if (sp->hdr.type == DbPageType::kFree) {
        memset((char *) sp + sizeof(sp->hdr), 0, m_pageSize - sizeof(sp->hdr));
    } else {
        assert(sp->hdr.type == DbPageType::kInvalid);
    }
    sp->hdr.type = sp->kPageType;
    sp->hdr.id = id;
    sp->sampleType = sampleType;
    sp->pageLastSample = (uint16_t) lastSample;
    sp->pageFirstTime = pageTime;
    auto spp = samplesPerPage(sampleType);
    setSamples(sp, 0, spp, fill);
}

//===========================================================================
void DbData::onWalApplySampleUpdate(
    void * ptr,
    size_t firstPos,
    size_t lastPos,
    double value,
    bool updateLast
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
    setSamples(sp, firstPos, lastPos, NAN);
    if (!isnan(value))
        setSample(sp, lastPos, value);
    if (updateLast)
        sp->pageLastSample = (uint16_t) lastPos;
}

//===========================================================================
void DbData::onWalApplySampleUpdateTime(void * ptr, Dim::TimePoint pageTime) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
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
    auto mp = txn.pin<MetricPage>(mi.infoPage);
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
    if (first > last)
        return noSamples(notify, id, name, stype, last, mi.interval);

    auto spp = samplesPerPage(stype);
    auto pageInterval = spp * mi.interval;
    auto numSamples = mp->retention / mp->interval;
    auto numPages = (numSamples - 1) / spp + 1;

    // Offset, in pages, from page being processed to the very last sample page.
    // Must be in [0, numPages - 1]
    auto poff = (mi.pageFirstTime - first + pageInterval - mi.interval)
        / pageInterval;

    pgno_t spno;
    unsigned sppos;
    if (first >= mi.pageFirstTime) {
        sppos = mp->lastPagePos;
        spno = mi.lastPage;
    } else {
        sppos = (uint32_t) (mp->lastPagePos + numPages - poff) % numPages;
        if (!radixFind(txn, &spno, mi.infoPage, sppos))
            spno = {};
    }

    DbSeriesInfo dsi;
    dsi.id = id;
    dsi.name = name;
    dsi.type = stype;
    dsi.interval = mi.interval;
    unsigned count = 0;
    for (;;) {
        assert(poff == (mi.pageFirstTime - first + pageInterval - mi.interval)
            / pageInterval);
        auto fpt = mi.pageFirstTime - poff * pageInterval;
        if (!spno) {
            // Missing page, interpreted as all NANs, which means there's
            // nothing to report and we just advance to first time on next page.
            first = fpt + pageInterval;
        } else {
            double value = NAN;
            const SamplePage * sp = nullptr;
            auto lastSample = spp - 1;
            if (spno > kMaxPageNum) {
                // Virtual page, get the cached value that is the same for every
                // sample on the page.
                if (sppos == mp->lastPagePos)
                    lastSample = mp->lastPageSample;
                value = getSample(&spno);
            } else {
                // Physical page, get values from the page
                sp = txn.pin<SamplePage>(spno);
                if (sppos == mp->lastPagePos) {
                    assert(sp->pageLastSample != spp);
                    lastSample = sp->pageLastSample;
                } else {
                    assert(fpt == sp->pageFirstTime);
                }
            }
            auto lastPageTime = fpt + lastSample * mi.interval;
            auto ent = (first - fpt) / mi.interval;
            if (poff == numPages) {
                // In the old samples section of the tip page in the ring
                // buffer.
                assert(ent);
                lastPageTime = fpt + pageInterval;
            }
            if (last < lastPageTime)
                lastPageTime = last;
            for (; first <= lastPageTime; first += mi.interval, ++ent) {
                if (sp) {
                    value = getSample(sp, ent);
                    if (isnan(value))
                        continue;
                }
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
        if (first > last)
            break;

        // advance to next page
        sppos = (sppos + 1) % numPages;
        radixFind(txn, &spno, mi.infoPage, sppos);
        poff -= 1;
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
// static
uint16_t DbData::entriesPerMetricPage(size_t pageSize) {
    auto off = offsetof(MetricPage, name) + metricNameSize(pageSize)
        + offsetof(RadixData, pages);
    return (uint16_t) (pageSize - off) / sizeof(*RadixData::pages);
}

//===========================================================================
// static
DbData::RadixData * DbData::radixData(MetricPage * mp, size_t pageSize) {
    auto ents = entriesPerMetricPage(pageSize);
    auto off = offsetof(RadixData, pages) + ents * sizeof(*RadixData::pages);
    auto ptr = (char *) mp + pageSize - off;
    return reinterpret_cast<DbData::RadixData *>(ptr);
}
