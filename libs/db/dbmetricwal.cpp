// Copyright Glen Knowles 2017 - 2026.
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
struct SampleInsertAtRec {    // Insert
    DbWal::Record hdr;
    uint16_t dstPos;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleInsertRec {      // InsertBack, InsertFront
    DbWal::Record hdr;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleEraseAtRec {   // Erase
    DbWal::Record hdr;
    uint16_t dstPos;
    uint16_t dstBits;
};
struct SampleEraseRec {   // EraseFront, EraseBack
    DbWal::Record hdr;
    uint16_t bitLen;
};
struct SampleReplaceAtRec { // Replace
    DbWal::Record hdr;
    uint16_t dstPos;
    uint16_t dstBits;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleReplaceRec {   // ReplaceFront, ReplaceBack
    DbWal::Record hdr;
    uint16_t dstBits;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};

struct SampleInsertAtTxnRec { // InsertTxn
    DbWalRecType type;
    pgno_t pgno;
    uint16_t dstPos;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleInsertTxnRec {   // InsertFrontTxn, InsertBackTxn
    DbWalRecType type;
    pgno_t pgno;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleEraseAtTxnRec {   // EraseTxn
    DbWalRecType type;
    pgno_t pgno;
    uint16_t dstPos;
    uint16_t dstBits;
};
struct SampleEraseTxnRec {   // EraseFrontTxn, EraseBackTxn
    DbWalRecType type;
    pgno_t pgno;
    uint16_t dstBits;
};
struct SampleReplaceAtTxnRec { // ReplaceTxn
    DbWalRecType type;
    pgno_t pgno;
    uint16_t dstPos;
    uint16_t dstBits;
    uint16_t srcBits;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};
struct SampleReplaceTxnRec { // ReplaceFrontTxn, ReplaceBackTxn
    DbWalRecType type;
    pgno_t pgno;
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
template <typename SampleRec>
static uint16_t sizeSampleData(const DbWal::Record & raw) {
    auto & rec = reinterpret_cast<const SampleRec &>(raw);
    return offsetof(SampleRec, data) + (rec.srcBits + 7) / 8;
}

//===========================================================================
static void applySampleEraseAt(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleEraseAtRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        rec->dstPos,
        rec->dstBits,
        {},
        0
    );
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

//===========================================================================
static void applySampleInsertAtTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInsertAtTxnRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        rec->dstPos,
        0,
        rec->data,
        rec->srcBits
    );
}

//===========================================================================
static void applySampleInsertFrontTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInsertTxnRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        0,
        0,
        rec->data,
        rec->srcBits
    );
}

//===========================================================================
static void applySampleInsertBackTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleInsertTxnRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        (size_t) -1,
        0,
        rec->data,
        rec->srcBits
    );
}

//===========================================================================
static void applySampleEraseAtTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleEraseAtTxnRec *>(args.rec);
    args.notify->onWalApplySampleReplace(
        args.page,
        rec->dstPos,
        rec->dstBits,
        {},
        0
    );
}

//===========================================================================
static void applySampleReplaceAtTxn(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const SampleReplaceAtTxnRec *>(args.rec);
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
    { kRecTypeSampleErase,
        DbWalRecInfo::sizeFn<SampleEraseAtRec>,
        applySampleEraseAt,
    },
    { kRecTypeSampleReplace,
        sizeSampleData<SampleReplaceAtRec>,
        applySampleReplaceAt,
    },
    { kRecTypeSampleInsertTxn,
        sizeSampleData<SampleInsertAtTxnRec>,
        applySampleInsertAtTxn,
        DbWalRecInfo::localTxnZeroFn,
    },
    { kRecTypeSampleInsertFrontTxn,
        sizeSampleData<SampleInsertTxnRec>,
        applySampleInsertFrontTxn,
        DbWalRecInfo::localTxnZeroFn,
    },
    { kRecTypeSampleInsertBackTxn,
        sizeSampleData<SampleInsertTxnRec>,
        applySampleInsertBackTxn,
        DbWalRecInfo::localTxnZeroFn,
    },
    { kRecTypeSampleEraseTxn,
        DbWalRecInfo::sizeFn<SampleEraseAtTxnRec>,
        applySampleEraseAtTxn,
        DbWalRecInfo::localTxnZeroFn,
    },
    { kRecTypeSampleReplaceTxn,
        sizeSampleData<SampleReplaceAtTxnRec>,
        applySampleReplaceAtTxn,
        DbWalRecInfo::localTxnZeroFn,
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
void DbTxn::walSampleReplaceTxn(
    pgno_t pgno,
    size_t dstPos,
    size_t dstBits,
    const uint8_t src[],
    size_t srcPos,
    size_t srcBits
) {
    if (m_txn)
        return walSampleReplace(pgno, dstPos, dstBits, src, srcPos, srcBits);

    union {
        DbWal::Record * rec;
        SampleEraseAtTxnRec * e;
        SampleReplaceAtTxnRec * r;
    } ptr;
    size_t bytes;
    if (!srcBits) {
        tie(ptr.e, bytes) = alloc<SampleEraseAtTxnRec>(
            kRecTypeSampleEraseTxn,
            pgno
        );
        ptr.e->dstPos = (uint16_t) dstPos;
        ptr.e->dstBits = (uint16_t) dstBits;
    } else {
        auto srcBytes = (srcBits + 7) / 8;
        tie(ptr.r, bytes) = alloc<SampleReplaceAtTxnRec>(
            kRecTypeSampleReplaceTxn,
            pgno,
            offsetof(SampleReplaceAtTxnRec, data) + srcBytes
        );
        ptr.r->dstPos = (uint16_t) dstPos;
        ptr.r->dstBits = (uint16_t) dstBits;
        ptr.r->srcBits = (uint16_t) srcBits;
        BitSpan::copy(ptr.r->data, 0, src, srcPos, srcBits);
    }
    m_wal.walAndApply({}, ptr.rec, bytes);
}

//===========================================================================
void DbTxn::walSampleReplace(
    pgno_t pgno,
    size_t dstPos,
    size_t dstBits,
    const uint8_t src[],
    size_t srcPos,
    size_t srcBits
) {
    assert(dstPos < numeric_limits<uint16_t>::max());
    assert(dstPos + dstBits < numeric_limits<uint16_t>::max());
    assert(srcPos < numeric_limits<uint16_t>::max());
    assert(srcPos + srcBits < numeric_limits<uint16_t>::max());
    if (!srcBits) {
        auto [rec, bytes] = alloc<SampleEraseAtRec>(kRecTypeSampleErase, pgno);
        rec->dstPos = (uint16_t) dstPos;
        rec->dstBits = (uint16_t) dstBits;
        wal(&rec->hdr, bytes);
        return;
    }

    auto srcBytes = (srcBits + 7) / 8;
    auto [rec, bytes] = alloc<SampleReplaceAtRec>(
        kRecTypeSampleReplace,
        pgno,
        offsetof(SampleReplaceAtRec, data) + srcBytes
    );
    rec->dstPos = (uint16_t) dstPos;
    rec->dstBits = (uint16_t) dstBits;
    rec->srcBits = (uint16_t) srcBits;
    BitSpan::copy(rec->data, 0, src, srcPos, srcBits);
    wal(&rec->hdr, bytes);
}
