// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdatametric.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

constexpr Duration kDefaultRetention = 7 * 24h;

const unsigned kMaxMetricNameLen = 1024;


/****************************************************************************
*
*   Private
*
***/

struct DbData::MetricPage : DbData::IndexPage {
    static const auto kPageType = DbPageType::kMetric;
    TimePoint creation;
    Duration retention;
    TimePoint lastIndexTime;
    uint16_t numSamples;

    // EXTENDS BEYOND END OF STRUCT
    DbSample samples[1]; // size(samples) == numSamples
};
static_assert(sizeof(DbData::MetricPage) <= kMinPageSize);

struct DbData::SamplePage : DbData::IndexPage {
    static const auto kPageType = DbPageType::kSample;
    uint8_t bitsUnused;

    // EXTENDS BEYOND END OF STRUCT
    unsigned char data[1]; // size(data) == IndexPage::bytesUsed
};


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCount = uperf("db.metrics (total)");

static auto & s_perfAncient = uperf("db.samples ignored (old)");
static auto & s_perfDup = uperf("db.samples ignored (same)");
static auto & s_perfNan = uperf("db.samples ignored (no value)");
static auto & s_perfChange = uperf("db.samples changed");
static auto & s_perfAdd = uperf("db.samples added");


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void noSamples(
    IDbDataNotify * notify,
    uint32_t id,
    string_view name,
    TimePoint first
) {
    if (notify) {
        DbSeriesInfo info{};
        info.id = id;
        info.name = name;
        info.first = first;
        info.last = first;
        if (notify->onDbSeriesStart(info))
            notify->onDbSeriesEnd(id);
    }
}

//===========================================================================
static bool reportSample(
    IDbDataNotify * notify,
    unsigned * count,
    DbSeriesInfo * dsi,
    const DbSample & sample
) {
    if (!*count++) {
        dsi->first = sample.time;
        if (!notify->onDbSeriesStart(*dsi))
            return false;
    }
    return notify->onDbSample(dsi->id, sample.time, sample.value);
}


/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
bool DbData::findMetricInfoPage(
    const DbTxn & txn,
    pgno_t * out,
    uint32_t id
) const {
    string data;
    shared_lock lk{m_mndxMut};
    return radixFind(txn, out, m_infoIndexRoot, id);
}

//===========================================================================
bool DbData::findMetricName(
    const DbTxn & txn,
    string * out,
    uint32_t id
) const {
    shared_lock lk{m_mndxMut};
    return indexFind(txn, out, m_nameIndexRoot, toKey(id));
}

//===========================================================================
bool DbData::findMetricId(
    const DbTxn & txn,
    uint32_t * out,
    string_view name
) const {
    string data;
    shared_lock lk{m_mndxMut};
    return indexFind(txn, &data, m_infoIndexRoot, name)
        && fromKey(out, data);
}

//===========================================================================
void DbData::metricClearCounters() {
    s_perfCount -= m_numMetrics;
}

//===========================================================================
size_t DbData::metricNameSize() const {
    assert(m_pageSize > sizeof(DbData::MetricPage) + sizeof(DbData::RadixData));
    auto count = m_pageSize - sizeof(DbData::MetricPage)
        - sizeof(DbData::RadixData);
    if (count > kMaxMetricNameLen)
        count = kMaxMetricNameLen;
    return count;
}

//===========================================================================
void DbData::metricDestructPage (DbTxn & txn, pgno_t pgno) {
    indexDestructPage(txn, pgno);

    s_perfCount -= 1;
}

//===========================================================================
bool DbData::loadMetrics (
    DbTxn & txn,
    IDbDataNotify * notify,
    pgno_t pgno
) {
    if (!pgno)
        return true;

    auto p = txn.viewPage<DbPageHeader>(pgno);
    if (!p)
        return false;

    if (p->type == DbPageType::kRadix) {
        auto rp = reinterpret_cast<const RadixPage *>(p);
        for (auto && mpno : rp->rd) {
            if (!loadMetrics(txn, notify, mpno))
                return false;
        }
        return true;
    }

    if (p->type != DbPageType::kMetric) {
        logMsgError() << "Bad metric page #" << pgno << ", type "
            << (unsigned)p->type;
        return false;
    }

    // metric page
    auto mp = reinterpret_cast<const MetricPage *>(p);
    string name;
    if (!indexFind(txn, &name, m_nameIndexRoot, toKey(mp->hdr.id))) {
        logMsgError() << "Name not found for metric #" << mp->hdr.id
            << ", at page #" << pgno;
        return false;
    }
    if (notify) {
        DbSeriesInfoEx info;
        info.id = mp->hdr.id;
        info.name = name;
        info.last = info.first + mp->retention;
        info.retention = mp->retention;
        info.creation = mp->creation;
        if (!notify->onDbSeriesStart(info))
            return false;
    }
    if (appStopping())
        return false;

    s_perfCount += 1;
    m_numMetrics += 1;
    return true;
}

//===========================================================================
void DbData::insertMetric(DbTxn & txn, uint32_t id, string_view name) {
    assert(!name.empty());
    auto nameLen = metricNameSize();
    if (name.size() >= nameLen)
        name = name.substr(0, nameLen - 1);

    // set info page
    auto pgno = allocPgno(txn);
    txn.logMetricInit(
        pgno,
        id,
        timeNow(),
        kDefaultRetention
    );

    // update indexes
    scoped_lock lk{m_mndxMut};
    bool inserted [[maybe_unused]] = radixInsertOrAssign(
        txn,
        m_infoIndexRoot,
        id,
        pgno
    );
    assert(inserted);
    auto data = string_view{ (char *)&id, sizeof(id) };
    inserted = indexUpdate(txn, m_idIndexRoot, name, data);
    assert(inserted);
    inserted = indexUpdate(txn, m_nameIndexRoot, data, name);
    assert(inserted);
    m_numMetrics += 1;

    s_perfCount += 1;
}

//===========================================================================
void DbData::onLogApplyMetricInit(
    void * ptr,
    uint32_t id,
    TimePoint creation,
    Duration retention
) {
    auto mp = static_cast<MetricPage *>(ptr);
    if (mp->hdr.type == DbPageType::kFree) {
        memset((char *) mp + sizeof(mp->hdr), 0, m_pageSize - sizeof(mp->hdr));
    } else {
        assert(mp->hdr.type == DbPageType::kInvalid);
    }
    mp->hdr.type = mp->kPageType;
    mp->hdr.id = id;
    mp->ndxAvail = (uint16_t) m_pageSize - offsetof(MetricPage, samples);
    mp->creation = creation;
    mp->retention = retention;
}

//===========================================================================
bool DbData::eraseMetric(string * name, DbTxn & txn, uint32_t id) {
    auto key = toKey(id);

    scoped_lock lk{m_mndxMut};
    if (!indexFind(txn, name, m_nameIndexRoot, key))
        return false;

    m_numMetrics -= 1;
    indexErase(txn, m_nameIndexRoot, key);
    indexErase(txn, m_idIndexRoot, *name);
    auto rp = txn.viewPage<RadixPage>(m_infoIndexRoot);
    radixErase(txn, rp->hdr, id, id + 1);
    return true;
}

//===========================================================================
void DbData::updateMetric(
    DbTxn & txn,
    uint32_t id,
    const DbMetricInfo & from
) {
    // TODO: validate retention

    pgno_t mpno;
    if (!findMetricInfoPage(txn, &mpno, id))
        return;

    auto mp = txn.viewPage<MetricPage>(mpno);
    DbMetricInfo info = {};
    info.retention = from.retention.count() ? from.retention : mp->retention;
    info.creation = from.creation ? from.creation : mp->creation;
    if (mp->retention != info.retention
        || mp->creation != info.creation
    ) {
        txn.logMetricUpdate(
            mpno,
            info.creation,
            info.retention
        );
    }
}

//===========================================================================
void DbData::getMetricInfo(
    IDbDataNotify * notify,
    const DbTxn & txn,
    uint32_t id
) {
    pgno_t mpno;
    if (!findMetricInfoPage(txn, &mpno, id))
        return noSamples(notify, id, {}, {});
    string name;
    bool found [[maybe_unused]] = findMetricName(txn, &name, id);
    assert(found);

    auto mp = txn.viewPage<MetricPage>(mpno);
    DbSeriesInfoEx info;
    info.id = id;
    info.name = name;
    if (auto num = mp->numSamples) {
        info.last = mp->samples[num - 1].time;
        info.first = info.last - mp->retention;
    } else {
        info.last = info.first + mp->retention;
    }
    info.retention = mp->retention;
    info.creation = mp->creation;
    if (notify->onDbSeriesStart(info))
        notify->onDbSeriesEnd(id);
}

//===========================================================================
void DbData::onLogApplyMetricUpdate(
    void * ptr,
    TimePoint creation,
    Duration retention
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    mp->creation = creation;
    mp->retention = retention;
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
size_t DbData::maxSamples(const MetricPage * mp) const {
    return mp->ndxAvail / sizeof(*mp->samples) + mp->numSamples;
}

//===========================================================================
size_t DbData::maxData(const SamplePage * sp) const {
    return m_pageSize - offsetof(SamplePage, data);
}

//===========================================================================
void DbData::onLogApplyMetricEraseSamples(
    void * ptr,
    size_t count,
    TimePoint lastIndexTime
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    if (lastIndexTime)
        mp->lastIndexTime = lastIndexTime;
    if (count) {
        if (count == -1) {
            memset(mp->samples, 0, mp->numSamples * sizeof(*mp->samples));
            mp->numSamples = 0;
        } else {
            assert(count < mp->numSamples);
            mp->numSamples -= (uint16_t) count;
            memmove(
                mp->samples,
                mp->samples + count,
                mp->numSamples * sizeof(*mp->samples)
            );
            memset(
                mp->samples + mp->numSamples,
                0,
                count * sizeof(*mp->samples)
            );
        }
    }
}

//===========================================================================
void DbData::onLogApplyMetricUpdateSample(
    void * ptr,
    size_t pos,
    double value,
    double dv
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    assert(pos < mp->numSamples);
    if (isfinite(dv)) {
        mp->samples[pos].value += dv;
    } else {
        mp->samples[pos].value = value;
    }
}

//===========================================================================
void DbData::onLogApplyMetricInsertSample(
    void * ptr,
    size_t pos,
    Duration dt,
    double value,
    double dv
) {
    auto mp = static_cast<MetricPage *>(ptr);
    assert(mp->hdr.type == mp->kPageType);
    assert(mp->numSamples < maxSamples(mp));

    if (pos >= mp->numSamples) {
        assert(pos == mp->numSamples);
    } else {
        copy_backward(
            mp->samples + pos,
            mp->samples + mp->numSamples,
            mp->samples + pos + 1
        );
    }
    auto & s = mp->samples[pos];
    if (!pos) {
        s = { TimePoint(dt), dv };
    } else {
        s = mp->samples[pos - 1];
        s.time += dt;
        if (isfinite(dv)) {
            s.value += dv;
        } else {
            s.value = value;
        }
    }
    assert(pos == mp->numSamples || s.time < mp->samples[pos + 1].time);
    mp->numSamples += 1;
    return;
}

//===========================================================================
void DbData::updateSample(
    DbTxn & txn,
    uint32_t id,
    TimePoint time,
    double value
) {
    if (isnan(value)) {
        s_perfNan += 1;
        return;
    }
    pgno_t mpno;
    if (!findMetricInfoPage(txn, &mpno, id))
        return;
    auto mp = txn.viewPage<MetricPage>(mpno);

    auto firstSampleTime = mp->numSamples
        ? mp->samples[mp->numSamples - 1].time - mp->retention
        : TimePoint{};
    if (time < firstSampleTime) {
        // sample older than retention, ignore it
        s_perfAncient += 1;
        return;
    }
    auto ii = equal_range(
        mp->samples,
        mp->samples + mp->numSamples,
        DbSample{time},
        [](auto & a, auto & b) { return a.time < b.time; }
    );
    auto pos = ii.first - mp->samples;
    if (ii.first != ii.second) {
        if (value == ii.first->value) {
            s_perfDup += 1;
        } else {
            s_perfChange += 1;
            txn.logMetricUpdateSample(
                mpno,
                pos,
                value,
                ii.first->value
            );
        }
        return;
    }
    if (mp->numSamples == maxSamples(mp)) {
        if (mp->samples[0].time < firstSampleTime) {
            assert(!mp->lastIndexTime);
            auto i = lower_bound(
                mp->samples,
                mp->samples + mp->numSamples,
                DbSample{firstSampleTime},
                [](auto & a, auto & b) { return a.time < b.time; }
            );
            auto count = i - mp->samples;
            txn.logMetricEraseSamples(mpno, count);
            assert(count <= pos);
            pos -= count;
        } else {
            sampleIndexErase(txn, mpno, {}, firstSampleTime);
            sampleIndexMerge(txn, mpno, mp->samples, mp->numSamples);
            txn.logMetricEraseSamples(
                mpno,
                (size_t) -1,
                mp->samples[pos - 1].time
            );
            pos = 0;
        }
    }

    s_perfAdd += 1;
    auto prev = pos ? mp->samples[pos - 1] : DbSample{};
    auto dt = time - prev.time;
    txn.logMetricInsertSample(mpno, pos, dt, value, prev.value);
    return;
}

//===========================================================================
void DbData::sampleIndexSplit(
    DbTxn & txn,
    pgno_t mpno,
    const DbData::SamplePage * sp
) {
    // Add additional sample page to the index and move half of the data on
    // sp to it.
    assert(!"Not implemented");
}

//===========================================================================
void DbData::sampleIndexErase(
    DbTxn & txn,
    pgno_t mpno,
    TimePoint first,
    TimePoint last
) {
    assert(!"Not implemented");
}

//===========================================================================
void DbData::sampleIndexMerge(
    DbTxn & txn,
    pgno_t mpno,
    const DbSample samples[],
    size_t count
) {
    assert(count);
    pgno_t spno;
    auto mp = txn.viewPage<SamplePage>(mpno);
    for (size_t base = 0; count; count = base) {
        if (!indexFindLeafPgno(txn, &spno, mpno, toKey(samples[count].time))) {
            // Data completely before all existing samples in index.
            //  - add new page to contain new samples
            //  - merge samples onto new page
            spno = allocPgno(txn);
            txn.logSampleInit(spno, mp->hdr.id);
            auto inserted [[maybe_unused]] =
                indexInsertLeafPgno(txn, mpno, toKey(samples[base].time), spno);
            assert(inserted);
        }

        auto sp = txn.viewPage<SamplePage>(spno);
        auto unpack = DbUnpackIter(sp->data, sp->ndxUsed, sp->bitsUnused);
        base = 0;
        if (unpack) {
            auto si = lower_bound(
                samples,
                samples + count,
                DbSample{unpack->time},
                [](auto & a, auto & b) { return a.time < b.time; }
            );
            base = si - samples;
        }

        auto prev = DbUnpackIter{}; // points to last sample before update
        auto time = samples[base].time;
        for (; unpack; ++unpack) {
            if (unpack->time >= time) {
                if (unpack->time == time)
                    ++unpack;
                break;
            }
            prev = unpack;
        }

        auto before = (size_t) 0;
        if (prev) {
            before = maxData(sp)
                - sp->ndxUsed + prev.size()
                - (prev.unusedBits() > 0);
        }
        string buf(maxData(sp) - before, '\0');
        if (prev.unusedBits())
            buf[0] = sp->data[before];
        DbPack pack(prev);
        pack.retarget(buf.data(), buf.size(), prev.unusedBits());

        auto mergeStep = [&](const DbSample & samp) {
            if (pack.put(samp.time, samp.value))
                return;
            if (pack.size()) {
                txn.logSampleUpdate(
                    sp->hdr.pgno,
                    before,
                    pack.view(),
                    pack.unusedBits()
                );
            }
            spno = allocPgno(txn);
            txn.logSampleInit(spno, mp->hdr.id);
            auto inserted [[maybe_unused]] =
                indexInsertLeafPgno(txn, mpno, toKey(samp.time), spno);
            assert(inserted);
            sp = txn.viewPage<SamplePage>(spno);
            buf.assign(maxData(sp), '\0');
            before = 0;
            pack = DbPack(buf.data(), buf.size(), 0);
        };

        auto pos = base;
        for (; unpack; ++unpack) {
            auto && samp = *unpack;
            while (samples[pos].time <= samp.time) {
                mergeStep(samples[pos]);
                pos += 1;
                if (samples[pos].time == samp.time) {
                    if (!++unpack)
                        goto REST_OF_SAMPLES;
                }
                if (pos == count)
                    goto REST_OF_UNPACK;
            }
            mergeStep(samp);
        }

    REST_OF_SAMPLES:
        for (; pos < count; ++pos)
            mergeStep(samples[pos]);
        goto NEXT_BASE;

    REST_OF_UNPACK:
        for (auto && samp : unpack)
            mergeStep(samp);

    NEXT_BASE:
        // log update
        txn.logSampleUpdate(
            sp->hdr.pgno,
            before,
            pack.view(),
            pack.unusedBits()
        );
    }
}

//===========================================================================
void DbData::onLogApplySampleInit(void * ptr, uint32_t id) {
    auto sp = static_cast<SamplePage *>(ptr);
    if (sp->hdr.type == DbPageType::kFree) {
        memset((char *) sp + sizeof(sp->hdr), 0, m_pageSize - sizeof(sp->hdr));
    } else {
        assert(sp->hdr.type == DbPageType::kInvalid);
    }
    sp->hdr.type = sp->kPageType;
    sp->hdr.id = id;
    sp->ndxAvail = (uint16_t) maxData(sp);
}

//===========================================================================
void DbData::onLogApplySampleUpdate(
    void * ptr,
    size_t offset,
    string_view data,
    size_t unusedBits
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
    auto count = data.size();
    assert(count && offset + count <= maxData(sp));
    memcpy(sp->data + offset, data.data(), count);
    sp->ndxUsed = (uint16_t) (offset + count);
    sp->ndxAvail = (uint16_t) maxData(sp) - sp->ndxUsed;
    assert(unusedBits < 8);
    sp->bitsUnused = (uint8_t) unusedBits;
}

//===========================================================================
void DbData::getSamples(
    DbTxn & txn,
    IDbDataNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    pgno_t mpno;
    if (!findMetricInfoPage(txn, &mpno, id))
        return noSamples(notify, id, {}, {});
    auto mp = txn.viewPage<MetricPage>(mpno);
    string name;
    bool found [[maybe_unused]] = findMetricName(txn, &name, id);
    assert(found);

    if (!mp->numSamples)
        return noSamples(notify, id, name, last);

    auto lastSampleTime = mp->samples[mp->numSamples - 1].time;
    auto firstSampleTime = lastSampleTime - mp->retention;
    if (first < firstSampleTime)
        first = firstSampleTime;
    if (last > lastSampleTime)
        last = lastSampleTime;
    if (first > last)
        return noSamples(notify, id, name, last);

    auto lower = lower_bound(
        mp->samples,
        mp->samples + mp->numSamples,
        DbSample{first},
        [](auto & a, auto & b) { return a.time < b.time; }
    );
    auto upper = upper_bound(
        mp->samples,
        mp->samples + mp->numSamples,
        DbSample{last},
        [](auto & a, auto & b) { return a.time < b.time; }
    );
    DbSeriesInfo dsi;
    dsi.id = id;
    dsi.name = name;
    dsi.last = last;
    unsigned count = 0;
    pgno_t spno;
    if (!indexFindLeafPgno(txn, &spno, mpno, toKey(first))
        && !indexFindLeafPgno(txn, &spno, mpno, toKey(first), true)
    ) {
        goto REST_OF_UNMERGED;
    }
    for (;;) {
        auto sp = txn.viewPage<SamplePage>(spno);
        auto unpack = DbUnpackIter(sp->data, sp->ndxUsed, sp->bitsUnused);
        for (; unpack; ++unpack) {
            if (unpack->time >= first)
                break;
        }
        for (; unpack; ++unpack) {
            if (unpack->time > last)
                goto REST_OF_UNMERGED;
            if (lower < upper) {
                while (lower->time < unpack->time) {
                    if (!reportSample(notify, &count, &dsi, *lower))
                        return;
                    if (++lower == upper)
                        goto REPORT_SAMPLE;
                }
                if (lower->time == unpack->time) {
                    if (!reportSample(notify, &count, &dsi, *lower++))
                        return;
                    continue;
                }
            }
        REPORT_SAMPLE:
            if (!reportSample(notify, &count, &dsi, *unpack))
                return;
        }

        // advance to next page
        if (!indexFindLeafPgno(txn, &spno, mpno, toKey(unpack->time), true))
            break;
    }

REST_OF_UNMERGED:
    for (; lower < upper; ++lower) {
        if (!count++) {
            dsi.first = lower->time;
            if (!notify->onDbSeriesStart(dsi))
                return;
        }
        if (!notify->onDbSample(id, lower->time, lower->value))
            return;
    }
    if (!count) {
        return noSamples(notify, id, name, last);
    } else {
        notify->onDbSeriesEnd(id);
    }
}
