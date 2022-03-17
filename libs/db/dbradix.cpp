// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbradix.cpp - tismet db
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

struct DbData::RadixPage {
    static const auto kPageType = DbPageType::kRadix;
    DbPageHeader hdr;

    // EXTENDS BEYOND END OF STRUCT
    RadixData rd;
};


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
// static
DbData::RadixData * DbData::radixData(
    DbPageHeader * hdr, 
    size_t pageSize
) {
    if (hdr->type == DbPageType::kMetric) {
        auto mp = reinterpret_cast<DbData::MetricPage *>(hdr);
        return radixData(mp, pageSize);
    } else {
        assert(hdr->type == DbPageType::kRadix);
        return &reinterpret_cast<DbData::RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
// static
const DbData::RadixData * DbData::radixData(
    const DbPageHeader * hdr,
    size_t pageSize
) {
    return radixData(const_cast<DbPageHeader *>(hdr), pageSize);
}

//===========================================================================
// static
uint16_t DbData::entriesPerRadixPage(size_t pageSize) {
    auto off = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    return (uint16_t) (pageSize - off) / sizeof(*RadixData::pages);
}

//===========================================================================
size_t DbData::radixPageEntries(
    int * out,
    size_t outLen,
    DbPageType rootType,
    uint16_t height,
    size_t pos
) {
    int * base = out;
    size_t pents = entriesPerRadixPage(m_pageSize);
    size_t rents;
    if (rootType == DbPageType::kMetric) {
        rents = entriesPerMetricPage(m_pageSize);
    } else {
        assert(rootType == DbPageType::kRadix);
        rents = pents;
    }

    for (;;) {
        *out++ = (int) (pos % pents);
        if (pos < rents)
            break;
        pos /= pents;
    }

    // always return at least "height" entries
    for (int * end = base + height + 1; out < end; ++out)
        *out = 0;
    reverse(base, out);
    return out - base;
}

//===========================================================================
void DbData::radixDestructPage(DbTxn & txn, pgno_t pgno) {
    auto rp = txn.viewPage<RadixPage>(pgno);
    radixDestruct(txn, rp->hdr);
}

//===========================================================================
void DbData::radixDestruct(DbTxn & txn, const DbPageHeader & hdr) {
    auto rd = radixData(&hdr, m_pageSize);
    for (auto && p : *rd) {
        if (p && p <= kMaxPageNum)
            freePage(txn, p);
    }
}

//===========================================================================
void DbData::radixErase(
    DbTxn & txn,
    pgno_t root,
    size_t firstPos,
    size_t lastPos
) {
    assert(firstPos <= lastPos);
    vector<pgno_t> pages;
    while (firstPos < lastPos) {
        const DbPageHeader * hdr;
        const RadixData * rd;
        size_t rpos;
        if (!radixFind(txn, &hdr, &rd, &rpos, root, firstPos))
            return;

        auto lastPagePos = min(
            (size_t) rd->numPages,
            rpos + lastPos - firstPos
        );
        pages.clear();
        pages.reserve(rd->numPages);
        for (auto i = rpos; i < lastPagePos; ++i) {
            if (auto p = rd->pages[i]; p && p <= kMaxPageNum) 
                pages.push_back(p);
        }
        if (!pages.empty()) {
            txn.walRadixErase(hdr->pgno, rpos, lastPagePos);
            for (auto&& p : pages)
                freePage(txn, p);
        }
        firstPos = firstPos + lastPagePos - rpos;
    }
}

//===========================================================================
bool DbData::radixInsertOrAssign(
    DbTxn & txn,
    pgno_t root,
    size_t pos,
    pgno_t value
) {
    assert(value);
    auto hdr = txn.viewPage<DbPageHeader>(root);
    auto rd = radixData(hdr, m_pageSize);
    auto id = hdr->id;

    int digits[10];
    size_t count = radixPageEntries(
        digits,
        size(digits),
        hdr->type,
        rd->height,
        pos
    );
    count -= 1;
    while (rd->height < count) {
        auto pgno = allocPgno(txn);
        txn.walRadixInit(
            pgno,
            id,
            rd->height,
            rd->pages,
            rd->pages + rd->numPages
        );
        txn.walRadixPromote(root, pgno);
        hdr = txn.viewPage<DbPageHeader>(root);
        rd = radixData(hdr, m_pageSize);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        auto pgno = rd->pages[pos];
        if (!pgno) {
            pgno = allocPgno(txn);
            txn.walRadixInit(
                pgno,
                id,
                rd->height - 1,
                nullptr,
                nullptr
            );
            txn.walRadixUpdate(hdr->pgno, pos, pgno);
        }
        hdr = txn.viewPage<DbPageHeader>(pgno);
        rd = radixData(hdr, m_pageSize);
        d += 1;
        count -= 1;
    }

    auto oval = rd->pages[*d];
    auto inserted = !oval;
    if (oval != value)
        txn.walRadixUpdate(hdr->pgno, *d, value);
    return inserted;
}

//===========================================================================
bool DbData::radixFind(
    DbTxn & txn,
    const DbPageHeader ** hdr,
    const RadixData ** rd,
    size_t * rpos,
    pgno_t root,
    size_t pos
) {
    *hdr = txn.viewPage<DbPageHeader>(root);
    *rd = radixData(*hdr, m_pageSize);

    int digits[10];
    size_t count = radixPageEntries(
        digits,
        size(digits),
        (*hdr)->type,
        (*rd)->height,
        pos
    );
    count -= 1;
    if ((*rd)->height < count) {
        // pos is beyond the limit that can be held in a tree this size, in
        // other words, it's past the end.
        return false;
    }
    int * d = digits;
    while (auto height = (*rd)->height) {
        int pos = (height > count) ? 0 : *d;
        if (!(*rd)->pages[pos]) {
            // Any zero value in a non-leaf page (since the stem pages are
            // fully populated up to the highest pos) means that we're past
            // the end.
            return false;
        }
        *hdr = txn.viewPage<DbPageHeader>((*rd)->pages[pos]);
        *rd = radixData(*hdr, m_pageSize);
        assert((*rd)->height == height - 1);
        if (height == count) {
            d += 1;
            count -= 1;
        }
    }

    *rpos = *d;
    return true;
}

//===========================================================================
bool DbData::radixFind(
    DbTxn & txn,
    pgno_t * out,
    pgno_t root,
    size_t pos
) {
    const DbPageHeader * hdr;
    const RadixData * rd;
    size_t rpos;
    if (radixFind(txn, &hdr, &rd, &rpos, root, pos)) {
        *out = rd->pages[rpos];
    } else {
        *out = {};
    }
    return *out;
}

//===========================================================================
static bool radixVisit(
    DbTxn & txn,
    uint32_t index,
    pgno_t root,
    const function<bool(DbTxn&, uint32_t index, pgno_t pgno)> & fn,
    size_t pageSize
) {
    auto hdr = txn.viewPage<DbPageHeader>(root);
    auto rd = DbData::radixData(hdr, pageSize);
    uint32_t step = 1;
    for (auto i = 0; i < rd->height; ++i) 
        step *= rd->numPages;
    for (auto && pgno : *rd) {
        if (pgno) {
            if (rd->height == 0) {
                if (!fn(txn, index, pgno))
                    return false;
            } else {
                if (!radixVisit(txn, index, pgno, fn, pageSize))
                    return false;
            }
        }
        index += step;
    }
    return true;
}

//===========================================================================
bool DbData::radixVisit(
    DbTxn & txn,
    pgno_t root,
    const function<bool(DbTxn&, uint32_t index, pgno_t pgno)> & fn
) {
    return ::radixVisit(txn, 0, root, fn, m_pageSize);
}


/****************************************************************************
*
*   DbWalRecInfo
*
***/

#pragma pack(push)
#pragma pack(1)

namespace {

//---------------------------------------------------------------------------
// Radix
struct RadixInitRec {
    DbWal::Record hdr;
    uint32_t id;
    uint16_t height;
};
struct RadixInitListRec {
    DbWal::Record hdr;
    uint32_t id;
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    pgno_t pages[1];
};
struct RadixEraseRec {
    DbWal::Record hdr;
    uint16_t firstPos;
    uint16_t lastPos;
};
struct RadixPromoteRec {
    DbWal::Record hdr;
    pgno_t refPage;
};
struct RadixUpdateRec {
    DbWal::Record hdr;
    uint16_t refPos;
    pgno_t refPage;
};

} // namespace

#pragma pack(pop)


static DbWalRegisterRec s_radixRecInfo = {
    { kRecTypeRadixInit,
        DbWalRecInfo::sizeFn<RadixInitRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const RadixInitRec *>(args.rec);
            args.notify->onWalApplyRadixInit(
                args.page,
                rec->id,
                rec->height,
                nullptr,
                nullptr
            );
        },
    },
    { kRecTypeRadixInitList,
        [](const DbWal::Record & raw) -> uint16_t {
            auto & rec = reinterpret_cast<const RadixInitListRec &>(raw);
            return offsetof(RadixInitListRec, pages)
                + rec.numPages * sizeof(*rec.pages);
        },
        [](auto args) {
            auto rec = reinterpret_cast<const RadixInitListRec *>(args.rec);
            args.notify->onWalApplyRadixInit(
                args.page,
                rec->id,
                rec->height,
                rec->pages,
                rec->pages + rec->numPages
            );
        },
    },
    { kRecTypeRadixErase,
        DbWalRecInfo::sizeFn<RadixEraseRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const RadixEraseRec *>(args.rec);
            args.notify->onWalApplyRadixErase(
                args.page,
                rec->firstPos,
                rec->lastPos
            );
        },
    },
    { kRecTypeRadixPromote,
        DbWalRecInfo::sizeFn<RadixPromoteRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const RadixPromoteRec *>(args.rec);
            args.notify->onWalApplyRadixPromote(args.page, rec->refPage);
        },
    },
    { kRecTypeRadixUpdate,
        DbWalRecInfo::sizeFn<RadixUpdateRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const RadixUpdateRec *>(args.rec);
            args.notify->onWalApplyRadixUpdate(
                args.page, 
                rec->refPos, 
                rec->refPage
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
void DbTxn::walRadixInit(
    pgno_t pgno,
    uint32_t id,
    uint16_t height,
    const pgno_t * firstPage,
    const pgno_t * lastPage
) {
    if (firstPage == lastPage) {
        auto [rec, bytes] = alloc<RadixInitRec>(kRecTypeRadixInit, pgno);
        rec->id = id;
        rec->height = height;
        wal(&rec->hdr, bytes);
        return;
    }

    auto count = lastPage - firstPage;
    assert(count <= numeric_limits<uint16_t>::max());
    auto extra = count * sizeof(*firstPage);
    auto offset = offsetof(RadixInitListRec, pages);
    auto [rec, bytes] = alloc<RadixInitListRec>(
        kRecTypeRadixInitList,
        pgno,
        offset + extra
    );
    rec->id = id;
    rec->height = height;
    rec->numPages = (uint16_t) count;
    memcpy(rec->pages, firstPage, extra);
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walRadixErase(
    pgno_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    auto [rec, bytes] = alloc<RadixEraseRec>(kRecTypeRadixErase, pgno);
    rec->firstPos = (uint16_t) firstPos;
    rec->lastPos = (uint16_t) lastPos;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walRadixPromote(pgno_t pgno, pgno_t refPage) {
    auto [rec, bytes] = alloc<RadixPromoteRec>(kRecTypeRadixPromote, pgno);
    rec->refPage = refPage;
    wal(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::walRadixUpdate(
    pgno_t pgno,
    size_t refPos,
    pgno_t refPage
) {
    auto [rec, bytes] = alloc<RadixUpdateRec>(kRecTypeRadixUpdate, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    wal(&rec->hdr, bytes);
}


/****************************************************************************
*
*   Radix wal apply
*
***/

//===========================================================================
void DbData::onWalApplyRadixInit(
    void * ptr,
    uint32_t id,
    uint16_t height,
    const pgno_t * firstPgno,
    const pgno_t * lastPgno
) {
    auto rp = static_cast<RadixPage *>(ptr);
    if (rp->hdr.type == DbPageType::kFree) {
        memset((char *) rp + sizeof(rp->hdr), 0, m_pageSize - sizeof(rp->hdr));
    } else {
        assert(rp->hdr.type == DbPageType::kInvalid);
    }
    rp->hdr.type = rp->kPageType;
    rp->hdr.id = id;
    rp->rd.height = height;
    rp->rd.numPages = entriesPerRadixPage(m_pageSize);
    if (auto count = lastPgno - firstPgno) {
        assert(count <= rp->rd.numPages);
        memcpy(rp->rd.pages, firstPgno, count * sizeof(*firstPgno));
    }
}

//===========================================================================
void DbData::onWalApplyRadixErase(
    void * ptr,
    size_t firstPos,
    size_t lastPos
) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr, m_pageSize);
    assert(firstPos < lastPos);
    assert(lastPos <= rd->numPages);
    memset(rd->pages + firstPos, 0, (lastPos - firstPos) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onWalApplyRadixPromote(void * ptr, pgno_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr, m_pageSize);
    rd->height += 1;
    rd->pages[0] = refPage;
    memset(rd->pages + 1, 0, (rd->numPages - 1) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onWalApplyRadixUpdate(void * ptr, size_t pos, pgno_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr, m_pageSize);
    assert(pos < rd->numPages);
    rd->pages[pos] = refPage;
}

