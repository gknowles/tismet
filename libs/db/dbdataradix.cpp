// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdataradix.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/


/****************************************************************************
*
*   Private
*
***/


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   DbData
*
***/


/****************************************************************************
*
*   Radix index
*
***/

//===========================================================================
DbData::RadixData * DbData::radixData(MetricPage * mp) const {
    auto ents = entriesPerMetricPage();
    auto off = offsetof(RadixData, pages) + ents * sizeof(*RadixData::pages);
    auto ptr = (char *) mp + m_pageSize - off;
    return reinterpret_cast<DbData::RadixData *>(ptr);
}

//===========================================================================
DbData::RadixData * DbData::radixData(DbPageHeader * hdr) const {
    if (hdr->type == DbPageType::kMetric) {
        auto mp = reinterpret_cast<DbData::MetricPage *>(hdr);
        return radixData(mp);
    } else {
        assert(hdr->type == DbPageType::kRadix);
        return &reinterpret_cast<DbData::RadixPage *>(hdr)->rd;
    }
}

//===========================================================================
const DbData::RadixData * DbData::radixData(const DbPageHeader * hdr) const {
    return radixData(const_cast<DbPageHeader *>(hdr));
}

//===========================================================================
uint16_t DbData::entriesPerRadixPage() const {
    auto off = offsetof(RadixPage, rd) + offsetof(RadixData, pages);
    return (uint16_t) (m_pageSize - off) / sizeof(*RadixData::pages);
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
    size_t pents = entriesPerRadixPage();
    size_t rents;
    if (rootType == DbPageType::kMetric) {
        rents = entriesPerMetricPage();
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
    auto rd = radixData(&hdr);
    for (auto && p : *rd) {
        if (p && p <= kMaxPageNum)
            freePage(txn, p);
    }
}

//===========================================================================
void DbData::radixErase(
    DbTxn & txn,
    const DbPageHeader & rhdr,
    size_t firstPos,
    size_t lastPos
) {
    assert(firstPos <= lastPos);
    while (firstPos < lastPos) {
        const DbPageHeader * hdr;
        const RadixData * rd;
        size_t rpos;
        if (!radixFind(txn, &hdr, &rd, &rpos, rhdr.pgno, firstPos))
            return;

        auto lastPagePos = min(
            (size_t) rd->numPages,
            rpos + lastPos - firstPos
        );
        bool changed = false;
        for (auto i = rpos; i < lastPagePos; ++i) {
            if (auto p = rd->pages[i]) {
                changed = true;
                if (p <= kMaxPageNum)
                    freePage(txn, p);
            }
        }
        if (changed)
            txn.logRadixErase(hdr->pgno, rpos, lastPagePos);
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
    auto id = hdr->id;
    auto rd = radixData(hdr);

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
        txn.logRadixInit(
            pgno,
            id,
            rd->height,
            rd->pages,
            rd->pages + rd->numPages
        );
        txn.logRadixPromote(root, pgno);
    }
    int * d = digits;
    while (count) {
        int pos = (rd->height > count) ? 0 : *d;
        auto pgno = rd->pages[pos];
        if (!pgno) {
            pgno = allocPgno(txn);
            txn.logRadixInit(
                pgno,
                id,
                rd->height - 1,
                nullptr,
                nullptr
            );
            txn.logRadixUpdate(hdr->pgno, pos, pgno);
        }
        hdr = txn.viewPage<DbPageHeader>(pgno);
        rd = radixData(hdr);
        d += 1;
        count -= 1;
    }

    auto oval = rd->pages[*d];
    auto inserted = !oval;
    if (oval != value)
        txn.logRadixUpdate(hdr->pgno, *d, value);
    return inserted;
}

//===========================================================================
void DbData::onLogApplyRadixInit(
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
    rp->rd.numPages = entriesPerRadixPage();
    if (auto count = lastPgno - firstPgno) {
        assert(count <= rp->rd.numPages);
        memcpy(rp->rd.pages, firstPgno, count * sizeof(*firstPgno));
    }
}

//===========================================================================
void DbData::onLogApplyRadixErase(
    void * ptr,
    size_t firstPos,
    size_t lastPos
) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr);
    assert(firstPos < lastPos);
    assert(lastPos <= rd->numPages);
    memset(rd->pages + firstPos, 0, (lastPos - firstPos) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onLogApplyRadixPromote(void * ptr, pgno_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr);
    rd->height += 1;
    rd->pages[0] = refPage;
    memset(rd->pages + 1, 0, (rd->numPages - 1) * sizeof(*rd->pages));
}

//===========================================================================
void DbData::onLogApplyRadixUpdate(void * ptr, size_t pos, pgno_t refPage) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    assert(hdr->type == DbPageType::kMetric || hdr->type == DbPageType::kRadix);
    auto rd = radixData(hdr);
    assert(pos < rd->numPages);
    rd->pages[pos] = refPage;
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
    *rd = radixData(*hdr);

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
        *rd = radixData(*hdr);
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
