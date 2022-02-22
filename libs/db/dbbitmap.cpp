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

#pragma pack(push)
#pragma pack(1)

namespace {

//---------------------------------------------------------------------------
// Bitmap
struct BitInitRec {
    DbLog::Record hdr;
    uint32_t id;
    uint32_t pos;
    bool fill;
};
struct BitUpdateRec {
    DbLog::Record hdr;
    uint32_t pos;
};

} // namespace

#pragma pack(pop)

//---------------------------------------------------------------------------
// DbData

struct DbData::BitmapPage {
    static const auto kPageType = DbPageType::kBitmap;
    DbPageHeader hdr;
};


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
[[maybe_unused]]
static size_t bitmapBitsPerPage(size_t pageSize) {
    auto offset = sizeof DbData::BitmapPage + sizeof uint64_t;
    offset -= offset % sizeof uint64_t;
    return (pageSize - offset) * CHAR_BIT;
}

//===========================================================================
static BitView bitmapView(void * hdr, size_t pageSize) {
    auto offset = sizeof DbData::BitmapPage + sizeof uint64_t;
    offset -= offset % sizeof uint64_t;
    auto base = (uint64_t *) ((char *) hdr + offset);
    auto words = (pageSize - offset) / sizeof uint64_t;
    return {base, words};
}


/****************************************************************************
*
*   DbData
*
***/

//===========================================================================
void DbData::bitUpdate(
    DbTxn & txn, 
    pgno_t root, 
    uint32_t id, 
    size_t pos, 
    bool value
) {
    auto bpp = bitmapBitsPerPage(m_pageSize);
    auto rpos = pos / bpp;
    auto bpos = pos % bpp;
    auto bpno = pgno_t{};
    radixFind(txn, &bpno, root, rpos);
    if (bpno) {
        if (!value) {
            auto hdr = txn.viewPage<BitmapPage>(bpno);
            auto bits = bitmapView(const_cast<BitmapPage *>(hdr), m_pageSize);
            if (bits[bpos] && bits.count() == 1) {
                radixErase(txn, hdr->hdr, rpos, rpos + 1);
                return;
            }
        }
        txn.logBitUpdate(bpno, bpos, value);
    } else {
        assert(value);
        bpno = allocPgno(txn);
        txn.logBitInit(bpno, id, false, bpos);
        radixInsertOrAssign(txn, root, rpos, bpno);
    }
}

//===========================================================================
static bool addBits(
    DbTxn & txn, 
    UnsignedSet * out,
    const DbPageHeader & hdr,
    size_t pageSize
) {
    if (hdr.type != DbPageType::kBitmap) {
        logMsgError() << "Bad bitmap page #" << hdr.pgno;
        return false;
    }
    auto bpp = bitmapBitsPerPage(pageSize);
    auto bits = bitmapView(const_cast<DbPageHeader *>(&hdr), pageSize);
    for (auto first = bits.find(0); first != bits.npos; ) {
        auto last = bits.findZero(first);
        if (last == bits.npos)
            last = bpp;
        out->insert(hdr.pgno + (unsigned) first, unsigned(last - first));
        first = bits.find(last);
    }
    return true;
}

//===========================================================================
bool DbData::bitLoad(DbTxn & txn, UnsignedSet * out, pgno_t root) {
    return radixVisit(
        txn, 
        root, 
        [out, pageSize = m_pageSize](DbTxn & txn, const DbPageHeader & hdr) {
            return addBits(txn, out, hdr, pageSize);
    });
}

//===========================================================================
void DbData::onLogApplyBitInit(
    void * ptr,
    uint32_t id,
    bool fill,
    uint32_t pos
) {
    auto bp = static_cast<BitmapPage *>(ptr);
    bp->hdr.type = bp->kPageType;
    bp->hdr.id = id;
    auto bits = bitmapView(bp, m_pageSize);
    if (fill) 
        bits.set();
    if (pos != numeric_limits<uint32_t>::max()) 
        bits.set(pos, !fill);
}

//===========================================================================
void DbData::onLogApplyBitUpdate(
    void * ptr,
    uint32_t pos,
    bool value
) {
    auto bp = static_cast<BitmapPage *>(ptr);
    auto bits = bitmapView(bp, m_pageSize);
    assert(bits[pos] != value);
    bits.set(pos, value);
}


/****************************************************************************
*
*   DbLogRecInfo
*
***/

static DbLogRecInfo::Table s_bitRecInfo{
    { kRecTypeBitInit,
        DbLogRecInfo::sizeFn<BitInitRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<const BitInitRec &>(log);
            notify->onLogApplyBitInit(page, rec.id, rec.fill, rec.pos);
        },
    },
    { kRecTypeBitSet,
        DbLogRecInfo::sizeFn<BitUpdateRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<const BitUpdateRec &>(log);
            notify->onLogApplyBitUpdate(page, rec.pos, true);
        },
    },
    { kRecTypeBitReset,
        DbLogRecInfo::sizeFn<BitUpdateRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<const BitUpdateRec &>(log);
            notify->onLogApplyBitUpdate(page, rec.pos, false);
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logBitInit(pgno_t pgno, uint32_t id, bool fill, size_t pos) {
    auto [rec, bytes] = alloc<BitInitRec>(kRecTypeBitInit, pgno);
    rec->id = id;
    rec->fill = fill;
    rec->pos = (uint32_t) pos;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logBitUpdate(pgno_t pgno, size_t pos, bool value) {
    auto [rec, bytes] = alloc<BitUpdateRec>(
        value ? kRecTypeBitSet : kRecTypeBitReset, 
        pgno
    );
    rec->pos = (uint32_t) pos;
    log(&rec->hdr, bytes);
}
