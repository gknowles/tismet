// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// dbmetriclog.cpp - tismet db
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
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
    TimePoint creation;

    // EXTENDS BEYOND END OF STRUCT
    char name[1]; // has terminating null
};
struct MetricUpdateRec {
    DbLog::Record hdr;
    TimePoint creation;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
};
struct MetricUpdatePosRec {
    DbLog::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
};
struct MetricUpdatePosAndIndexRec {
    DbLog::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
    pgno_t refPage;
};

// Also an implicit transaction, non-standard format
struct MetricUpdateSampleTxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t refSample;
};

struct MetricUpdateSampleRec {
    DbLog::Record hdr;
    uint16_t refSample;
};
struct MetricUpdateSampleAndIndexRec {
    DbLog::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
    uint16_t refSample;
    pgno_t refPage;
};

//---------------------------------------------------------------------------
// Sample
struct SampleInitRec {
    DbLog::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    TimePoint pageTime;
    uint16_t lastSample;
};
struct SampleInitFillRec {
    DbLog::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    TimePoint pageTime;
    uint16_t lastSample;
    double value;
};
struct SampleUpdateRec {
    DbLog::Record hdr;
    uint16_t firstSample;
    uint16_t lastSample;
    double value;
};
struct SampleUpdateTimeRec {
    DbLog::Record hdr;
    TimePoint pageTime;
};

// Update (with or without last) is also an implicit transaction
struct SampleUpdateFloat64TxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    double value;
};
struct SampleUpdateFloat32TxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    float value;
};
struct SampleUpdateInt32TxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    int32_t value;
};
struct SampleUpdateInt16TxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    int16_t value;
};
struct SampleUpdateInt8TxnRec {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t pos;
    int8_t value;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLogRecInfo - Metric
*
***/

//===========================================================================
static uint16_t sizeMetricInit(const DbLog::Record & log) {
    auto & rec = reinterpret_cast<const MetricInitRec &>(log);
    return offsetof(MetricInitRec, name)
        + (uint16_t) strlen(rec.name) + 1;
}

//===========================================================================
static void applyMetricInit(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricInitRec *>(args.log);
    args.notify->onLogApplyMetricInit(
        args.page,
        rec->id,
        rec->name,
        rec->creation,
        rec->sampleType,
        rec->retention,
        rec->interval
    );
}

//===========================================================================
static void applyMetricUpdate(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateRec *>(args.log);
    args.notify->onLogApplyMetricUpdate(
        args.page,
        rec->creation,
        rec->sampleType,
        rec->retention,
        rec->interval
    );
}

//===========================================================================
static void applyMetricClearSamples(const DbLogApplyArgs & args) {
    args.notify->onLogApplyMetricClearSamples(args.page);
}

//===========================================================================
static void applyMetricUpdatePos(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdatePosRec *>(args.log);
    args.notify->onLogApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        (size_t) -1,
        {}
    );
}

//===========================================================================
static void applyMetricUpdatePosAndIndex(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdatePosAndIndexRec *>(args.log);
    args.notify->onLogApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        (size_t) -1,
        rec->refPage
    );
}

//===========================================================================
static void applyMetricUpdateSampleTxn(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleTxnRec *>(args.log);
    args.notify->onLogApplyMetricUpdateSamples(
        args.page,
        (size_t) -1,
        {},
        rec->refSample,
        {}
    );
}

//===========================================================================
static void applyMetricUpdateSample(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleRec *>(args.log);
    args.notify->onLogApplyMetricUpdateSamples(
        args.page,
        (size_t) -1,
        {},
        rec->refSample,
        {}
    );
}

//===========================================================================
static void applyMetricUpdateSampleAndIndex(const DbLogApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleAndIndexRec *>(args.log);
    args.notify->onLogApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        rec->refSample,
        rec->refPage
    );
}

static DbLogRecInfo::Table s_metricRecInfo {
    { kRecTypeMetricInit,
        sizeMetricInit,
        applyMetricInit,
    },
    { kRecTypeMetricUpdate,
        DbLogRecInfo::sizeFn<MetricUpdateRec>,
        applyMetricUpdate,
    },
    { kRecTypeMetricClearSamples,
        DbLogRecInfo::sizeFn<DbLog::Record>,
        applyMetricClearSamples,
    },
    { kRecTypeMetricUpdatePos,
        DbLogRecInfo::sizeFn<MetricUpdatePosRec>,
        applyMetricUpdatePos,
    },
    { kRecTypeMetricUpdatePosAndIndex,
        DbLogRecInfo::sizeFn<MetricUpdatePosAndIndexRec>,
        applyMetricUpdatePosAndIndex,
    },
    { kRecTypeMetricUpdateSampleTxn,
        DbLogRecInfo::sizeFn<MetricUpdateSampleTxnRec>,
        applyMetricUpdateSampleTxn,
    },
    { kRecTypeMetricUpdateSample,
        DbLogRecInfo::sizeFn<MetricUpdateSampleRec>,
        applyMetricUpdateSample,
    },
    { kRecTypeMetricUpdateSampleAndIndex,
        DbLogRecInfo::sizeFn<MetricUpdateSampleAndIndexRec>,
        applyMetricUpdateSampleAndIndex,
    },
};


/****************************************************************************
*
*   DbLogRecInfo - Sample
*
***/

//===========================================================================
APPLY(SampleInit) {
    auto rec = reinterpret_cast<const SampleInitRec *>(args.log);
    args.notify->onLogApplySampleInit(
        args.page,
        rec->id,
        rec->sampleType,
        rec->pageTime,
        rec->lastSample,
        NAN
    );
}

//===========================================================================
APPLY(SampleInitFill) {
    auto rec = reinterpret_cast<const SampleInitFillRec *>(args.log);
    args.notify->onLogApplySampleInit(
        args.page,
        rec->id,
        rec->sampleType,
        rec->pageTime,
        rec->lastSample,
        rec->value
    );
}

//===========================================================================
APPLY(SampleUpdate) {
    auto rec = reinterpret_cast<const SampleUpdateRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->firstSample,
        rec->lastSample,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateLast) {
    auto rec = reinterpret_cast<const SampleUpdateRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->firstSample,
        rec->lastSample,
        rec->value,
        true
    );
}

//===========================================================================
APPLY(SampleUpdateTime) {
    auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(args.log);
    args.notify->onLogApplySampleUpdateTime(args.page, rec->pageTime);
}

//===========================================================================
APPLY(SampleUpdateFloat32Txn) {
    auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateFloat64Txn) {
    auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateInt8Txn) {
    auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateInt16Txn) {
    auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateInt32Txn) {
    auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
APPLY(SampleUpdateFloat32LastTxn) {
    auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
APPLY(SampleUpdateFloat64LastTxn) {
    auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
APPLY(SampleUpdateInt8LastTxn) {
    auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
APPLY(SampleUpdateInt16LastTxn) {
    auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
APPLY(SampleUpdateInt32LastTxn) {
    auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(args.log);
    args.notify->onLogApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}


static DbLogRecInfo::Table s_sampleRecInfo{
    { kRecTypeSampleInit,
        DbLogRecInfo::sizeFn<SampleInitRec>,
        applySampleInit,
    },
    { kRecTypeSampleInitFill,
        DbLogRecInfo::sizeFn<SampleInitFillRec>,
        applySampleInitFill,
    },
    { kRecTypeSampleUpdate,
        DbLogRecInfo::sizeFn<SampleUpdateRec>,
        applySampleUpdate,
    },
    { kRecTypeSampleUpdateLast,
        DbLogRecInfo::sizeFn<SampleUpdateRec>,
        applySampleUpdateLast,
    },
    { kRecTypeSampleUpdateTime,
        DbLogRecInfo::sizeFn<SampleUpdateTimeRec>,
        applySampleUpdateTime,
    },
    { kRecTypeSampleUpdateFloat32Txn,
        DbLogRecInfo::sizeFn<SampleUpdateFloat32TxnRec>,
        applySampleUpdateFloat32Txn,
    },
    { kRecTypeSampleUpdateFloat64Txn,
        DbLogRecInfo::sizeFn<SampleUpdateFloat64TxnRec>,
        applySampleUpdateFloat64Txn,
    },
    { kRecTypeSampleUpdateInt8Txn,
        DbLogRecInfo::sizeFn<SampleUpdateInt8TxnRec>,
        applySampleUpdateInt8Txn,
    },
    { kRecTypeSampleUpdateInt16Txn,
        DbLogRecInfo::sizeFn<SampleUpdateInt16TxnRec>,
        applySampleUpdateInt16Txn,
    },
    { kRecTypeSampleUpdateInt32Txn,
        DbLogRecInfo::sizeFn<SampleUpdateInt32TxnRec>,
        applySampleUpdateInt32Txn,
    },
    { kRecTypeSampleUpdateFloat32LastTxn,
        DbLogRecInfo::sizeFn<SampleUpdateFloat32TxnRec>,
        applySampleUpdateFloat32LastTxn,
    },
    { kRecTypeSampleUpdateFloat64LastTxn,
        DbLogRecInfo::sizeFn<SampleUpdateFloat64TxnRec>,
        applySampleUpdateFloat64LastTxn,
    },
    { kRecTypeSampleUpdateInt8LastTxn,
        DbLogRecInfo::sizeFn<SampleUpdateInt8TxnRec>,
        applySampleUpdateInt8LastTxn,
    },
    { kRecTypeSampleUpdateInt16LastTxn,
        DbLogRecInfo::sizeFn<SampleUpdateInt16TxnRec>,
        applySampleUpdateInt16LastTxn,
    },
    { kRecTypeSampleUpdateInt32LastTxn,
        DbLogRecInfo::sizeFn<SampleUpdateInt32TxnRec>,
        applySampleUpdateInt32LastTxn,
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
    string_view name,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto extra = name.size() + 1;
    auto offset = offsetof(MetricInitRec, name);
    auto [rec, bytes] = alloc<MetricInitRec>(
        kRecTypeMetricInit,
        pgno,
        offset + extra
    );
    rec->id = id;
    rec->sampleType = sampleType;
    rec->retention = retention;
    rec->interval = interval;
    rec->creation = creation;
    memcpy(rec->name, name.data(), extra - 1);
    rec->name[extra] = 0;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdate(
    pgno_t pgno,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto [rec, bytes] = alloc<MetricUpdateRec>(kRecTypeMetricUpdate, pgno);
    rec->creation = creation;
    rec->sampleType = sampleType;
    rec->retention = retention;
    rec->interval = interval;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricClearSamples(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeMetricClearSamples, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdateSamplesTxn(pgno_t pgno, size_t refSample) {
    if (m_txn)
        return logMetricUpdateSamples(pgno, (size_t) -1, {}, refSample, {});

    MetricUpdateSampleTxnRec rec;
    rec.type = kRecTypeMetricUpdateSampleTxn;
    rec.pgno = pgno;
    rec.refSample = (uint16_t) refSample;
    m_log.logAndApply(0, (DbLog::Record *) &rec, sizeof(rec));
}

//===========================================================================
void DbTxn::logMetricUpdateSamples(
    pgno_t pgno,
    size_t refPos,
    TimePoint refTime,
    size_t refSample,
    pgno_t refPage
) {
    if (empty(refTime)) {
        assert(refPos == -1 && !refPage);
        auto [rec, bytes] =
            alloc<MetricUpdateSampleRec>(kRecTypeMetricUpdateSample, pgno);
        rec->refSample = (uint16_t) refSample;
        return log(&rec->hdr, bytes);
    }
    if (refSample != -1) {
        assert(refPos != -1);
        auto [rec, bytes] = alloc<MetricUpdateSampleAndIndexRec>(
            kRecTypeMetricUpdateSampleAndIndex,
            pgno
        );
        rec->refPos = (uint16_t) refPos;
        rec->refTime = refTime;
        rec->refSample = (uint16_t) refSample;
        rec->refPage = refPage;
        return log(&rec->hdr, bytes);
    }
    if (!refPage) {
        assert(refPos != -1 && refSample == -1);
        auto [rec, bytes] =
            alloc<MetricUpdatePosRec>(kRecTypeMetricUpdatePos, pgno);
        rec->refPos = (uint16_t) refPos;
        rec->refTime = refTime;
        return log(&rec->hdr, bytes);
    }
    assert(refPos != -1);
    auto [rec, bytes] = alloc<MetricUpdatePosAndIndexRec>(
        kRecTypeMetricUpdatePosAndIndex,
        pgno
    );
    rec->refPos = (uint16_t) refPos;
    rec->refTime = refTime;
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleInit(
    pgno_t pgno,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample
) {
    auto [rec, bytes] = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    rec->sampleType = sampleType;
    rec->pageTime = pageTime;
    rec->lastSample = (uint16_t) lastSample;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleInit(
    pgno_t pgno,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample,
    double fill
) {
    auto [rec, bytes] = alloc<SampleInitFillRec>(kRecTypeSampleInitFill, pgno);
    rec->id = id;
    rec->sampleType = sampleType;
    rec->pageTime = pageTime;
    rec->lastSample = (uint16_t) lastSample;
    rec->value = fill;
    log(&rec->hdr, bytes);
}

//===========================================================================
// This one is not like the others, it represents a transaction with just
// a single value update.
void DbTxn::logSampleUpdateTxn(
    pgno_t pgno,
    size_t pos,
    double value,
    bool updateLast
) {
    if (m_txn)
        return logSampleUpdate(pgno, pos, pos, value, updateLast);

    union {
        SampleUpdateFloat32TxnRec f32;
        SampleUpdateFloat64TxnRec f64;
        SampleUpdateInt8TxnRec i8;
        SampleUpdateInt16TxnRec i16;
        SampleUpdateInt32TxnRec i32;
    } tmp;
    assert(pos <= numeric_limits<decltype(tmp.i8.pos)>::max());
    size_t bytes{0};
    tmp.i8.pgno = pgno;
    tmp.i8.pos = (uint16_t) pos;
    if (auto ival = (int32_t) value; ival != value) {
        if (auto fval = (float) value; fval == value) {
            tmp.f32.type = updateLast
                ? kRecTypeSampleUpdateFloat32LastTxn
                : kRecTypeSampleUpdateFloat32Txn;
            tmp.f32.value = fval;
            bytes = sizeof(tmp.f32);
        } else {
            tmp.f64.type = updateLast
                ? kRecTypeSampleUpdateFloat64LastTxn
                : kRecTypeSampleUpdateFloat64Txn;
            tmp.f64.value = value;
            bytes = sizeof(tmp.f64);
        }
    } else {
        if ((int8_t) ival == ival) {
            tmp.i8.type = updateLast
                ? kRecTypeSampleUpdateInt8LastTxn
                : kRecTypeSampleUpdateInt8Txn;
            tmp.i8.value = (int8_t) ival;
            bytes = sizeof(tmp.i8);
        } else if ((int16_t) ival == ival) {
            tmp.i16.type = updateLast
                ? kRecTypeSampleUpdateInt16LastTxn
                : kRecTypeSampleUpdateInt16Txn;
            tmp.i16.value = (int16_t) ival;
            bytes = sizeof(tmp.i16);
        } else {
            tmp.i32.type = updateLast
                ? kRecTypeSampleUpdateInt32LastTxn
                : kRecTypeSampleUpdateInt32Txn;
            tmp.i32.value = ival;
            bytes = sizeof(tmp.i32);
        }
    }
    m_log.logAndApply(0, (DbLog::Record *) &tmp, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdate(
    pgno_t pgno,
    size_t firstSample,
    size_t lastSample,
    double value,
    bool updateLast
) {
    auto type = updateLast ? kRecTypeSampleUpdateLast : kRecTypeSampleUpdate;
    auto [rec, bytes] = alloc<SampleUpdateRec>(type, pgno);
    assert(firstSample <= lastSample);
    assert(lastSample <= numeric_limits<decltype(rec->firstSample)>::max());
    rec->firstSample = (uint16_t) firstSample;
    rec->lastSample = (uint16_t) lastSample;
    rec->value = value;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdateTime(pgno_t pgno, TimePoint pageTime) {
    auto [rec, bytes] = 
        alloc<SampleUpdateTimeRec>(kRecTypeSampleUpdateTime, pgno);
    rec->pageTime = pageTime;
    log(&rec->hdr, bytes);
}
