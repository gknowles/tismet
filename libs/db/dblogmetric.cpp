// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogmetric.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

#pragma pack(push)
#pragma pack(1)

namespace {

//---------------------------------------------------------------------------
// Metric
struct MetricInitRec {
    DbLog::Record hdr;
    uint32_t id;
    TimePoint creation;
    Duration retention;
};
struct MetricUpdateRec {
    DbLog::Record hdr;
    TimePoint creation;
    Duration retention;
};
struct MetricEraseSamplesRec {
    DbLog::Record hdr;
    uint16_t count;
    TimePoint lastIndexTime;
};
struct MetricUpdateSampleRec {
    DbLog::Record hdr;
    uint16_t pos;
    double value;
};
struct MetricInsertSampleRec {
    DbLog::Record hdr;
    uint16_t pos;
    Duration dt;
    double value;
};

// Also an implicit transaction, non-standard format
struct MetricInsertSampleTxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    Duration dt;
    double value;
};

//---------------------------------------------------------------------------
// Sample
struct SampleInitRec {
    DbLog::Record hdr;
    uint32_t id;
};
struct SampleBulkUpdateRec {
    DbLog::Record hdr;
    uint16_t pos;
    uint16_t count;
    uint8_t unusedBits;

    // EXTENDS BEYOND END OF STRUCT
    char data[1];
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLogRecInfo
*
***/

static DbLogRecInfo::Table s_metricRecInfo{
    { kRecTypeMetricInit,
        DbLogRecInfo::sizeFn<MetricInitRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricInitRec const &>(log);
            return notify->onLogApplyMetricInit(
                page,
                rec.id,
                rec.creation,
                rec.retention
            );
        },
    },
    { kRecTypeMetricUpdate,
        DbLogRecInfo::sizeFn<MetricUpdateRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricUpdateRec const &>(log);
            return notify->onLogApplyMetricUpdate(
                page,
                rec.creation,
                rec.retention
            );
        },
    },
    { kRecTypeMetricEraseSamples,
        DbLogRecInfo::sizeFn<MetricEraseSamplesRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricEraseSamplesRec const &>(log);
            return notify->onLogApplyMetricEraseSamples(
                page,
                rec.count,
                rec.lastIndexTime
            );
        },
    },
    { kRecTypeMetricUpdateSample,
        DbLogRecInfo::sizeFn<MetricUpdateSampleRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricUpdateSampleRec const &>(log);
            return notify->onLogApplyMetricUpdateSample(
                page,
                rec.pos,
                rec.value,
                NAN
            );
        },
    },
    { kRecTypeMetricInsertSample,
        DbLogRecInfo::sizeFn<MetricInsertSampleRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricInsertSampleRec const &>(log);
            return notify->onLogApplyMetricInsertSample(
                page,
                rec.pos,
                rec.dt,
                rec.value,
                NAN
            );
        },
    },
    { kRecTypeMetricInsertSampleTxn,
        DbLogRecInfo::sizeFn<MetricInsertSampleTxnRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<MetricInsertSampleTxnRec const &>(log);
            return notify->onLogApplyMetricInsertSample(
                page,
                rec.pos,
                rec.dt,
                rec.value,
                NAN
            );
        },
    },
};

static DbLogRecInfo::Table s_sampleRecInfo{
    { kRecTypeSampleInit,
        DbLogRecInfo::sizeFn<SampleInitRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<SampleInitRec const &>(log);
            return notify->onLogApplySampleInit(page, rec.id);
        },
    },
    { kRecTypeSampleBulkUpdate,
        [](auto & log) -> uint16_t {
            auto & rec = reinterpret_cast<SampleBulkUpdateRec const &>(log);
            return offsetof(SampleBulkUpdateRec, data) + rec.count;
        },
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<SampleBulkUpdateRec const &>(log);
            return notify->onLogApplySampleUpdate(
                page,
                rec.pos,
                {rec.data, rec.count},
                rec.unusedBits
            );
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logMetricInit(
    pgno_t pgno,
    uint32_t id,
    TimePoint creation,
    Duration retention
) {
    auto [rec, bytes] = alloc<MetricInitRec>(kRecTypeMetricInit, pgno);
    rec->id = id;
    rec->creation = creation;
    rec->retention = retention;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdate(
    pgno_t pgno,
    TimePoint creation,
    Duration retention
) {
    auto [rec, bytes] = alloc<MetricUpdateRec>(kRecTypeMetricUpdate, pgno);
    rec->creation = creation;
    rec->retention = retention;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricEraseSamples(
    pgno_t pgno,
    size_t count,
    TimePoint lastIndexTime
) {
    auto [rec, bytes] = alloc<MetricEraseSamplesRec>(
        kRecTypeMetricEraseSamples,
        pgno
    );
    rec->count = (uint16_t) count;
    rec->lastIndexTime = lastIndexTime;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdateSample(
    pgno_t pgno,
    size_t pos,
    double value,
    double oldValue
) {
    auto [rec, bytes] = alloc<MetricUpdateSampleRec>(
        kRecTypeMetricUpdateSample,
        pgno
    );
    rec->pos = (uint16_t) pos;
    rec->value = value;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricInsertSample(
    pgno_t pgno,
    size_t pos,
    Dim::Duration dt,
    double value,
    double oldValue
) {
    auto [rec, bytes] = alloc<MetricInsertSampleRec>(
        kRecTypeMetricInsertSample,
        pgno
    );
    rec->pos = (uint16_t) pos;
    rec->dt = dt;
    rec->value = value;
    log(&rec->hdr, bytes);
}

//===========================================================================
// This one is not like the others, it represents a transaction with just
// a single value insert.
void DbTxn::logMetricInsertSampleTxn(
    pgno_t pgno,
    size_t pos,
    Dim::Duration dt,
    double value,
    double oldValue
) {
    if (m_txn)
        return logMetricInsertSample(pgno, pos, dt, value, oldValue);

    MetricInsertSampleTxnRec rec{};
    rec.type = kRecTypeMetricInsertSampleTxn;
    rec.pgno = pgno;
    rec.pos = (uint16_t) pos;
    rec.dt = dt;
    rec.value = value;
    m_log.logAndApply(0, (DbLog::Record *) &rec, sizeof(rec));
}

//===========================================================================
void DbTxn::logSampleInit(pgno_t pgno, uint32_t id) {
    auto [rec, bytes] = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdate(
    pgno_t pgno,
    size_t offset,
    string_view data,
    size_t unusedBits
) {
    auto [rec, bytes] = alloc<SampleBulkUpdateRec>(
        kRecTypeSampleBulkUpdate,
        pgno,
        offsetof(SampleBulkUpdateRec, data) + data.size()
    );
    rec->pos = (uint16_t) offset;
    rec->count = (uint16_t) data.size();
    rec->unusedBits = (uint8_t) unusedBits;
    memcpy(rec->data, data.data(), data.size());
    log(&rec->hdr, bytes);
}
