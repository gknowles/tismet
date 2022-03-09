// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbmetricwal.cpp - tismet db
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
    DbWal::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
    TimePoint creation;

    // EXTENDS BEYOND END OF STRUCT
    char name[1]; // has terminating null
};
struct MetricUpdateRec {
    DbWal::Record hdr;
    TimePoint creation;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
};
struct MetricUpdatePosRec {
    DbWal::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
};
struct MetricUpdatePosAndIndexRec {
    DbWal::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
    pgno_t refPage;
};

// Also an implicit transaction, non-standard format
struct MetricUpdateSampleTxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t refSample;
};

struct MetricUpdateSampleRec {
    DbWal::Record hdr;
    uint16_t refSample;
};
struct MetricUpdateSampleAndIndexRec {
    DbWal::Record hdr;
    uint16_t refPos;
    TimePoint refTime;
    uint16_t refSample;
    pgno_t refPage;
};

//---------------------------------------------------------------------------
// Sample
struct SampleInitRec {
    DbWal::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    TimePoint pageTime;
    uint16_t lastSample;
};
struct SampleInitFillRec {
    DbWal::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    TimePoint pageTime;
    uint16_t lastSample;
    double value;
};
struct SampleUpdateRec {
    DbWal::Record hdr;
    uint16_t firstSample;
    uint16_t lastSample;
    double value;
};
struct SampleUpdateTimeRec {
    DbWal::Record hdr;
    TimePoint pageTime;
};

// Update (with or without last) is also an implicit transaction
struct SampleUpdateFloat64TxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t pos;
    double value;
};
struct SampleUpdateFloat32TxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t pos;
    float value;
};
struct SampleUpdateInt32TxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t pos;
    int32_t value;
};
struct SampleUpdateInt16TxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t pos;
    int16_t value;
};
struct SampleUpdateInt8TxnRec {
    DbWalRecType type;
    pgno_t pgno;
    uint16_t pos;
    int8_t value;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbWalRecInfo - Metric
*
***/

//===========================================================================
static uint16_t sizeMetricInit(const DbWal::Record & raw) {
    auto & rec = reinterpret_cast<const MetricInitRec &>(raw);
    return offsetof(MetricInitRec, name)
        + (uint16_t) strlen(rec.name) + 1;
}

//===========================================================================
static void applyMetricInit(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricInitRec *>(args.rec);
    args.notify->onWalApplyMetricInit(
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
static void applyMetricUpdate(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateRec *>(args.rec);
    args.notify->onWalApplyMetricUpdate(
        args.page,
        rec->creation,
        rec->sampleType,
        rec->retention,
        rec->interval
    );
}

//===========================================================================
static void applyMetricClearSamples(const DbWalApplyArgs & args) {
    args.notify->onWalApplyMetricClearSamples(args.page);
}

//===========================================================================
static void applyMetricUpdatePos(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdatePosRec *>(args.rec);
    args.notify->onWalApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        (size_t) -1,
        {}
    );
}

//===========================================================================
static void applyMetricUpdatePosAndIndex(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdatePosAndIndexRec *>(args.rec);
    args.notify->onWalApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        (size_t) -1,
        rec->refPage
    );
}

//===========================================================================
static void applyMetricUpdateSampleTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleTxnRec *>(args.rec);
    args.notify->onWalApplyMetricUpdateSamples(
        args.page,
        (size_t) -1,
        {},
        rec->refSample,
        {}
    );
}

//===========================================================================
static void applyMetricUpdateSample(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleRec *>(args.rec);
    args.notify->onWalApplyMetricUpdateSamples(
        args.page,
        (size_t) -1,
        {},
        rec->refSample,
        {}
    );
}

//===========================================================================
static void applyMetricUpdateSampleAndIndex(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const MetricUpdateSampleAndIndexRec *>(args.rec);
    args.notify->onWalApplyMetricUpdateSamples(
        args.page,
        rec->refPos,
        rec->refTime,
        rec->refSample,
        rec->refPage
    );
}

static DbWalRecInfo::Table s_metricRecInfo {
    { kRecTypeMetricInit,
        sizeMetricInit,
        applyMetricInit,
    },
    { kRecTypeMetricUpdate,
        DbWalRecInfo::sizeFn<MetricUpdateRec>,
        applyMetricUpdate,
    },
    { kRecTypeMetricClearSamples,
        DbWalRecInfo::sizeFn<DbWal::Record>,
        applyMetricClearSamples,
    },
    { kRecTypeMetricUpdatePos,
        DbWalRecInfo::sizeFn<MetricUpdatePosRec>,
        applyMetricUpdatePos,
    },
    { kRecTypeMetricUpdatePosAndIndex,
        DbWalRecInfo::sizeFn<MetricUpdatePosAndIndexRec>,
        applyMetricUpdatePosAndIndex,
    },
    { kRecTypeMetricUpdateSampleTxn,
        DbWalRecInfo::sizeFn<MetricUpdateSampleTxnRec>,
        applyMetricUpdateSampleTxn,
    },
    { kRecTypeMetricUpdateSample,
        DbWalRecInfo::sizeFn<MetricUpdateSampleRec>,
        applyMetricUpdateSample,
    },
    { kRecTypeMetricUpdateSampleAndIndex,
        DbWalRecInfo::sizeFn<MetricUpdateSampleAndIndexRec>,
        applyMetricUpdateSampleAndIndex,
    },
};


/****************************************************************************
*
*   DbWalRecInfo - Sample
*
***/

//===========================================================================
static void applySampleInit(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInitRec *>(args.rec);
    args.notify->onWalApplySampleInit(
        args.page,
        rec->id,
        rec->sampleType,
        rec->pageTime,
        rec->lastSample,
        NAN
    );
}

//===========================================================================
static void applySampleInitFill(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInitFillRec *>(args.rec);
    args.notify->onWalApplySampleInit(
        args.page,
        rec->id,
        rec->sampleType,
        rec->pageTime,
        rec->lastSample,
        rec->value
    );
}

//===========================================================================
static void applySampleUpdate(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->firstSample,
        rec->lastSample,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateLast(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->firstSample,
        rec->lastSample,
        rec->value,
        true
    );
}

//===========================================================================
static void applySampleUpdateTime(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(args.rec);
    args.notify->onWalApplySampleUpdateTime(args.page, rec->pageTime);
}

//===========================================================================
static void applySampleUpdateFloat32Txn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateFloat64Txn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateInt8Txn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateInt16Txn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateInt32Txn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        false
    );
}

//===========================================================================
static void applySampleUpdateFloat32LastTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
static void applySampleUpdateFloat64LastTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
static void applySampleUpdateInt8LastTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
static void applySampleUpdateInt16LastTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}

//===========================================================================
static void applySampleUpdateInt32LastTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(args.rec);
    args.notify->onWalApplySampleUpdate(
        args.page,
        rec->pos,
        rec->pos,
        rec->value,
        true
    );
}


static DbWalRecInfo::Table s_sampleRecInfo{
    { kRecTypeSampleInit,
        DbWalRecInfo::sizeFn<SampleInitRec>,
        applySampleInit,
    },
    { kRecTypeSampleInitFill,
        DbWalRecInfo::sizeFn<SampleInitFillRec>,
        applySampleInitFill,
    },
    { kRecTypeSampleUpdate,
        DbWalRecInfo::sizeFn<SampleUpdateRec>,
        applySampleUpdate,
    },
    { kRecTypeSampleUpdateLast,
        DbWalRecInfo::sizeFn<SampleUpdateRec>,
        applySampleUpdateLast,
    },
    { kRecTypeSampleUpdateTime,
        DbWalRecInfo::sizeFn<SampleUpdateTimeRec>,
        applySampleUpdateTime,
    },
    { kRecTypeSampleUpdateFloat32Txn,
        DbWalRecInfo::sizeFn<SampleUpdateFloat32TxnRec>,
        applySampleUpdateFloat32Txn,
    },
    { kRecTypeSampleUpdateFloat64Txn,
        DbWalRecInfo::sizeFn<SampleUpdateFloat64TxnRec>,
        applySampleUpdateFloat64Txn,
    },
    { kRecTypeSampleUpdateInt8Txn,
        DbWalRecInfo::sizeFn<SampleUpdateInt8TxnRec>,
        applySampleUpdateInt8Txn,
    },
    { kRecTypeSampleUpdateInt16Txn,
        DbWalRecInfo::sizeFn<SampleUpdateInt16TxnRec>,
        applySampleUpdateInt16Txn,
    },
    { kRecTypeSampleUpdateInt32Txn,
        DbWalRecInfo::sizeFn<SampleUpdateInt32TxnRec>,
        applySampleUpdateInt32Txn,
    },
    { kRecTypeSampleUpdateFloat32LastTxn,
        DbWalRecInfo::sizeFn<SampleUpdateFloat32TxnRec>,
        applySampleUpdateFloat32LastTxn,
    },
    { kRecTypeSampleUpdateFloat64LastTxn,
        DbWalRecInfo::sizeFn<SampleUpdateFloat64TxnRec>,
        applySampleUpdateFloat64LastTxn,
    },
    { kRecTypeSampleUpdateInt8LastTxn,
        DbWalRecInfo::sizeFn<SampleUpdateInt8TxnRec>,
        applySampleUpdateInt8LastTxn,
    },
    { kRecTypeSampleUpdateInt16LastTxn,
        DbWalRecInfo::sizeFn<SampleUpdateInt16TxnRec>,
        applySampleUpdateInt16LastTxn,
    },
    { kRecTypeSampleUpdateInt32LastTxn,
        DbWalRecInfo::sizeFn<SampleUpdateInt32TxnRec>,
        applySampleUpdateInt32LastTxn,
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::walMetricInit(
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
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walMetricUpdate(
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
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walMetricClearSamples(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbWal::Record>(kRecTypeMetricClearSamples, pgno);
    wal(rec, bytes);
}

//===========================================================================
void DbTxn::walMetricUpdateSamplesTxn(pgno_t pgno, size_t refSample) {
    if (m_txn)
        return walMetricUpdateSamples(pgno, (size_t) -1, {}, refSample, {});

    MetricUpdateSampleTxnRec rec;
    rec.type = kRecTypeMetricUpdateSampleTxn;
    rec.pgno = pgno;
    rec.refSample = (uint16_t) refSample;
    m_wal.walAndApply(0, (DbWal::Record *) &rec, sizeof(rec));
}

//===========================================================================
void DbTxn::walMetricUpdateSamples(
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
        return wal(&rec->hdr, bytes);
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
        return wal(&rec->hdr, bytes);
    }
    if (!refPage) {
        assert(refPos != -1 && refSample == -1);
        auto [rec, bytes] =
            alloc<MetricUpdatePosRec>(kRecTypeMetricUpdatePos, pgno);
        rec->refPos = (uint16_t) refPos;
        rec->refTime = refTime;
        return wal(&rec->hdr, bytes);
    }
    assert(refPos != -1);
    auto [rec, bytes] = alloc<MetricUpdatePosAndIndexRec>(
        kRecTypeMetricUpdatePosAndIndex,
        pgno
    );
    rec->refPos = (uint16_t) refPos;
    rec->refTime = refTime;
    rec->refPage = refPage;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walSampleInit(
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
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walSampleInit(
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
    wal(&rec->hdr, bytes);
}

//===========================================================================
// This one is not like the others, it represents a transaction with just
// a single value update.
void DbTxn::walSampleUpdateTxn(
    pgno_t pgno,
    size_t pos,
    double value,
    bool updateLast
) {
    if (m_txn)
        return walSampleUpdate(pgno, pos, pos, value, updateLast);

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
    m_wal.walAndApply(0, (DbWal::Record *) &tmp, bytes);
}

//===========================================================================
void DbTxn::walSampleUpdate(
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
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walSampleUpdateTime(pgno_t pgno, TimePoint pageTime) {
    auto [rec, bytes] = 
        alloc<SampleUpdateTimeRec>(kRecTypeSampleUpdateTime, pgno);
    rec->pageTime = pageTime;
    wal(&rec->hdr, bytes);
}
