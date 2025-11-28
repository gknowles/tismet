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
struct SampleUpdateRootRec {
    DbWal::Record hdr;
    pgno_t rootPage;
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
struct SampleDataAtRec {
    DbWal::Record hdr;
    uint16_t bitPos;
    uint16_t bitLen;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleDataRec {
    DbWal::Record hdr;
    uint16_t bitLen;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleDataRefRec {
    DbWal::Record hdr;
    uint16_t bitPos;
    uint16_t bitLen;
};
struct SampleDataLenRec {
    DbWal::Record hdr;
    uint16_t bitLen;
};
struct SampleReplaceAtRec {
    DbWal::Record hdr;
    uint16_t dstPos;
    uint16_t dstBits;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleReplaceRec {
    DbWal::Record hdr;
    uint16_t dstBits;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
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
static void applySampleUpdateRoot(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleUpdateRootRec *>(args.rec);
    args.notify->onWalApplySampleUpdateRoot(
        args.page,
        rec->rootPage
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
static uint16_t sizeSampleReplaceAt(const DbWal::Record & raw) {
    auto & rec = reinterpret_cast<const SampleReplaceAtRec &>(raw);
    return offsetof(SampleReplaceAtRec, data)
        + (rec.srcBits + 7) / 8;
}

//===========================================================================
static void applySampleReplaceAt(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleReplaceAtRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        rec->dstPos,
        rec->dstBits,
        rec->data,
        rec->srcBits
    );
}


static DbWalRegisterRec s_sampleRecInfo{
    { kRecTypeSampleInit,
        DbWalRecInfo::sizeFn<SampleInitRec>,
        applySampleInit,
    },
    { kRecTypeSampleUpdateRoot,
        DbWalRecInfo::sizeFn<SampleUpdateRootRec>,
        applySampleUpdateRoot,
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
    { kRecTypeSampleReplace,
        sizeSampleReplaceAt,
        applySampleReplaceAt,
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
    auto [rec, bytes] = alloc<SampleUpdateRootRec>(
        kRecTypeSampleUpdateRoot,
        pgno
    );
    rec->rootPage = newRoot;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walSampleUpdateTime(
    pgno_t pgno,
    TimePoint firstTime,
    TimePoint lastTime
) {
    if (empty(firstTime) && empty(lastTime))
        return;

    if (!empty(firstTime) && !empty(lastTime)) {
        auto [rec, bytes] = alloc<SampleUpdateTime2Rec>(
            kRecTypeSampleUpdateTime,
            pgno
        );
        rec->firstTime = firstTime;
        rec->lastTime = lastTime;
        wal(&rec->hdr, bytes);
        return;
    }

    auto type = kRecTypeSampleUpdateFirstTime;
    auto time = firstTime;
    if (empty(firstTime)) {
        type = kRecTypeSampleUpdateLastTime;
        time = lastTime;
    }
    auto [rec, bytes] = alloc<SampleUpdateTimeRec>(type, pgno);
    rec->pageTime = time;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walSampleReplace(
    pgno_t pgno,
    size_t dstPos,
    size_t dstBits,
    const uint8_t src[],
    size_t srcBits
) {
    assert(dstPos < numeric_limits<uint16_t>::max());
    assert(dstPos + dstBits < numeric_limits<uint16_t>::max());
    assert(srcBits < numeric_limits<uint16_t>::max());
    auto srcBytes = (srcBits + 7) / 8;
    auto [rec, bytes] = alloc<SampleReplaceAtRec>(
        kRecTypeSampleReplace,
        pgno,
        offsetof(SampleReplaceAtRec, data) + srcBytes
    );
    rec->dstPos = (uint16_t) dstPos;
    rec->dstBits = (uint16_t) dstBits;
    rec->srcBits = (uint16_t) srcBits;
    memcpy(rec->data, src, srcBytes);
    wal(&rec->hdr, bytes);
}
