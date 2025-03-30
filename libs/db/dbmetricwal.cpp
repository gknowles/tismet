// Copyright Glen Knowles 2017 - 2023.
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

#pragma pack(push, 1)

namespace {

//---------------------------------------------------------------------------
// Sample
struct SampleInitRec {
    DbWal::Record hdr;
    uint32_t id;
    DbSampleType type;
    TimePoint time;
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
struct SampleUpdateTime2Rec {
    DbWal::Record hdr;
    TimePoint firstTime;
    TimePoint lastTime;
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
*   DbWalRecInfo - Sample
*
***/

//===========================================================================
static void applySampleInit(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInitRec *>(args.rec);
    args.notify->onWalApplySampleInit(
        args.page,
        rec->id,
        rec->type,
        rec->time,
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
static void applySampleUpdateTimes(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateTime2Rec *>(args.rec);
    args.notify->onWalApplySampleUpdateTime(
        args.page,
        rec->firstTime,
        rec->lastTime
    );
}

//===========================================================================
static void applySampleUpdateFirstTime(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(args.rec);
    args.notify->onWalApplySampleUpdateTime(
        args.page,
        rec->pageTime,
        {}
    );
}

//===========================================================================
static void applySampleUpdateLastTime(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(args.rec);
    args.notify->onWalApplySampleUpdateTime(
        args.page,
        {},
        rec->pageTime
    );
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


static DbWalRegisterRec s_sampleRecInfo{
    { kRecTypeSampleInit,
        DbWalRecInfo::sizeFn<SampleInitRec>,
        applySampleInit,
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
        DbWalRecInfo::sizeFn<SampleUpdateTime2Rec>,
        applySampleUpdateTimes,
    },
    { kRecTypeSampleUpdateFirstTime,
        DbWalRecInfo::sizeFn<SampleUpdateTimeRec>,
        applySampleUpdateFirstTime,
    },
    { kRecTypeSampleUpdateLastTime,
        DbWalRecInfo::sizeFn<SampleUpdateTimeRec>,
        applySampleUpdateLastTime,
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
void DbTxn::walSampleInit(
    pgno_t pgno,
    uint32_t id,
    DbSampleType type,
    TimePoint time,
    double value
) {
    auto [rec, bytes] = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    rec->type = type;
    rec->time = time;
    rec->value = value;
    wal(&rec->hdr, bytes);
}

//===========================================================================
// This one is not like the others, it represents a transaction with just a
// single value update.
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
    m_wal.walAndApply({}, (DbWal::Record *) &tmp, bytes);
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
