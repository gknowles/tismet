// Copyright Glen Knowles 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogbitmap.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

//---------------------------------------------------------------------------
// DbData

struct DbData::BitmapPage {
    static const auto kPageType = DbPageType::kBitmap;
    DbPageHeader hdr;
    uint32_t base;

    // EXTENDS BEYOND END OF STRUCT
    uint64_t bits[1];
};


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
[[maybe_unused]]
static size_t bitmapBitsPerPage(size_t pageSize) {
    auto offset = offsetof(DbData::BitmapPage, bits);
    auto words = (pageSize - offset) / sizeof uint64_t;
    return words * 64;
}

//===========================================================================
static BitView bitmapView(void * hdr, size_t pageSize) {
    auto offset = offsetof(DbData::BitmapPage, bits);
    auto words = (pageSize - offset) / sizeof uint64_t;
    auto base = (uint64_t *) ((char *) hdr + offset);
    return {base, words};
}


/****************************************************************************
*
*   Bitmap index
*
***/

//===========================================================================
size_t DbData::bitsPerPage() const {
    return bitmapBitsPerPage(m_pageSize);
}

//===========================================================================
bool DbData::bitUpsert(
    DbTxn & txn, 
    pgno_t root, 
    uint32_t id, 
    size_t firstPos, 
    size_t lastPos,
    bool value
) {
    auto count = lastPos - firstPos;
    auto bpp = bitsPerPage();
    auto rpos = firstPos / bpp;
    auto bpos = firstPos % bpp;
    auto bpno = pgno_t{};
    radixFind(txn, &bpno, root, rpos);
    if (bpno) {
        auto hdr = txn.viewPage<BitmapPage>(bpno);
        auto bits = bitmapView(const_cast<BitmapPage *>(hdr), m_pageSize);
        auto num = bits.count(bpos, count);
        if (value) {
            if (num == count)
                return false;
        } else {
            if (!num)
                return false;
            if (num == bits.count()) {
                radixErase(txn, root, rpos, rpos + 1);
                return true;
            }
        }
        txn.logBitUpdate(bpno, bpos, bpos + count, value);
    } else {
        if (!value)
            return false;
        bpno = allocPgno(txn);
        txn.logBitInit(bpno, id, (uint32_t) rpos, false, bpos);
        if (count > 1)
            txn.logBitUpdate(bpno, bpos + 1, bpos + count, true);
        radixInsertOrAssign(txn, root, rpos, bpno);
    }
    return true;
}

//===========================================================================
static bool addBits(
    DbTxn & txn, 
    UnsignedSet * out,
    uint32_t index,
    const DbPageHeader & hdr,
    size_t pageSize
) {
    if (hdr.type != DbPageType::kBitmap) {
        logMsgError() << "Bad bitmap page #" << hdr.pgno << ", type"
            << (unsigned) hdr.type;
        return false;
    }
    auto bpp = bitmapBitsPerPage(pageSize);
    index *= (uint32_t) bpp;
    auto bits = bitmapView(const_cast<DbPageHeader *>(&hdr), pageSize);
    for (auto first = bits.find(0); first != bits.npos; ) {
        auto last = bits.findZero(first);
        if (last == bits.npos)
            last = bpp;
        out->insert(index + (unsigned) first, unsigned(last - first));
        first = bits.find(last);
    }
    return true;
}

//===========================================================================
bool DbData::bitLoad(DbTxn & txn, UnsignedSet * out, pgno_t root) {
    return radixVisit(
        txn, 
        root, 
        [out, pageSize = m_pageSize](DbTxn & txn, auto index, auto & hdr) {
            return addBits(txn, out, index, hdr, pageSize);
    });
}


/****************************************************************************
*
*   DbLogRecInfo
*
***/

#pragma pack(push)
#pragma pack(1)

namespace {

struct BitInitRec {
    DbLog::Record hdr;
    uint32_t id;
    uint32_t base;
    uint32_t pos;
    bool fill;
};
struct BitUpdateRec {
    DbLog::Record hdr;
    uint32_t pos;
};
struct BitUpdateRangeRec {
    DbLog::Record hdr;
    uint32_t firstPos;
    uint32_t lastPos;
    bool value;
};

} // namespace

#pragma pack(pop)


static DbLogRecInfo::Table s_bitRecInfo = {
    { kRecTypeBitInit,
        DbLogRecInfo::sizeFn<BitInitRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const BitInitRec *>(args.log);
            args.notify->onLogApplyBitInit(
                args.page, 
                rec->id, 
                rec->base, 
                rec->fill, 
                rec->pos
            );
        },
    },
    { kRecTypeBitSet,
        DbLogRecInfo::sizeFn<BitUpdateRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const BitUpdateRec *>(args.log);
            args.notify->onLogApplyBitUpdate(
                args.page, 
                rec->pos, 
                rec->pos + 1, 
                true
            );
        },
    },
    { kRecTypeBitReset,
        DbLogRecInfo::sizeFn<BitUpdateRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const BitUpdateRec *>(args.log);
            args.notify->onLogApplyBitUpdate(
                args.page, 
                rec->pos, 
                rec->pos + 1, 
                false
            );
        },
    },
    { kRecTypeBitUpdateRange,
        DbLogRecInfo::sizeFn<BitUpdateRangeRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const BitUpdateRangeRec *>(args.log);
            args.notify->onLogApplyBitUpdate(
                args.page, 
                rec->firstPos, 
                rec->lastPos, 
                rec->value
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
void DbTxn::logBitInit(
    pgno_t pgno, 
    uint32_t id, 
    uint32_t base, 
    bool fill, 
    size_t bpos
) {
    auto [rec, bytes] = alloc<BitInitRec>(kRecTypeBitInit, pgno);
    rec->id = id;
    rec->base = base;
    rec->fill = fill;
    rec->pos = (uint32_t) bpos;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logBitUpdate(
    pgno_t pgno, 
    size_t firstPos, 
    size_t lastPos, 
    bool value
) {
    if (firstPos + 1 == lastPos) {
        auto [rec, bytes] = alloc<BitUpdateRec>(
            value ? kRecTypeBitSet : kRecTypeBitReset, 
            pgno
        );
        rec->pos = (uint32_t) firstPos;
        log(&rec->hdr, bytes);
        return;
    }

    auto [rec, bytes] = alloc<BitUpdateRangeRec>(kRecTypeBitUpdateRange, pgno);
    rec->firstPos = (uint32_t) firstPos;
    rec->lastPos = (uint32_t) lastPos;
    rec->value = value;
    log(&rec->hdr, bytes);
}


/****************************************************************************
*
*   Bitmap log apply
*
***/

//===========================================================================
void DbData::onLogApplyBitInit(
    void * ptr,
    uint32_t id,
    uint32_t base,
    bool fill,
    uint32_t bpos
) {
    auto bp = static_cast<BitmapPage *>(ptr);
    if (bp->hdr.type == DbPageType::kFree) {
        memset((char *) bp + sizeof(bp->hdr), 0, m_pageSize - sizeof(bp->hdr));
    } else {
        assert(bp->hdr.type == DbPageType::kInvalid);
    }
    bp->hdr.type = bp->kPageType;
    bp->hdr.id = id;
    bp->base = base;
    auto bits = bitmapView(bp, m_pageSize);
    if (fill) 
        bits.set();
    if (bpos != numeric_limits<uint32_t>::max()) 
        bits.set(bpos, !fill);
}

//===========================================================================
void DbData::onLogApplyBitUpdate(
    void * ptr,
    uint32_t firstPos,
    uint32_t lastPos,
    bool value
) {
    auto bp = static_cast<BitmapPage *>(ptr);
    auto bits = bitmapView(bp, m_pageSize);
    bits.set(firstPos, lastPos - firstPos, value);
}
