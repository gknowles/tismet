// Copyright Glen Knowles 2017 - 2025.
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
void DbTxn::walSampleUpdateIndexRoot(pgno_t pgno, pgno_t newRoot) {
}

//===========================================================================
void DbTxn::walSampleUpdateTime(
    pgno_t pgno,
    Dim::TimePoint firstTime,
    Dim::TimePoint lastTime
) {
}

//===========================================================================
void DbTxn::walSampleReplace(
    pgno_t pgno,
    size_t dstPos,
    size_t dstBits,
    const uint8_t * src,
    size_t srcBits
) {
}
