// Copyright Glen Knowles 2017 - 2019.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogradix.cpp - tismet db
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
// Radix
struct RadixInitRec {
    DbLog::Record hdr;
    uint32_t id;
    uint16_t height;
};
struct RadixInitListRec {
    DbLog::Record hdr;
    uint32_t id;
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    pgno_t pages[1];
};
struct RadixEraseRec {
    DbLog::Record hdr;
    uint16_t firstPos;
    uint16_t lastPos;
};
struct RadixPromoteRec {
    DbLog::Record hdr;
    pgno_t refPage;
};
struct RadixUpdateRec {
    DbLog::Record hdr;
    uint16_t refPos;
    pgno_t refPage;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLogRecInfo
*
***/

//===========================================================================
static void applyRadixUpdate(
    DbLog::IApplyNotify * notify,
    void * page,
    DbLog::Record const & log
) {
    auto & rec = reinterpret_cast<RadixUpdateRec const &>(log);
    notify->onLogApplyRadixUpdate(page, rec.refPos, rec.refPage);
}

static DbLogRecInfo::Table s_radixRecInfo{
    { kRecTypeRadixInit,
        DbLogRecInfo::sizeFn<RadixInitRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<RadixInitRec const &>(log);
            notify->onLogApplyRadixInit(
                page,
                rec.id,
                rec.height,
                nullptr,
                nullptr
            );
        },
    },
    { kRecTypeRadixInitList,
        [](DbLog::Record const & log) -> uint16_t {
            auto & rec = reinterpret_cast<RadixInitListRec const &>(log);
            return offsetof(RadixInitListRec, pages)
                + rec.numPages * sizeof(*rec.pages);
        },
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<RadixInitListRec const &>(log);
            notify->onLogApplyRadixInit(
                page,
                rec.id,
                rec.height,
                rec.pages,
                rec.pages + rec.numPages
            );
        },
    },
    { kRecTypeRadixErase,
        DbLogRecInfo::sizeFn<RadixEraseRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<RadixEraseRec const &>(log);
            notify->onLogApplyRadixErase(
                page,
                rec.firstPos,
                rec.lastPos
            );
        },
    },
    { kRecTypeRadixPromote,
        DbLogRecInfo::sizeFn<RadixPromoteRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<RadixPromoteRec const &>(log);
            notify->onLogApplyRadixPromote(page, rec.refPage);
        },
    },
    { kRecTypeRadixUpdate,
        DbLogRecInfo::sizeFn<RadixUpdateRec>,
        applyRadixUpdate,
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logRadixInit(
    pgno_t pgno,
    uint32_t id,
    uint16_t height,
    pgno_t const * firstPage,
    pgno_t const * lastPage
) {
    if (firstPage == lastPage) {
        auto [rec, bytes] = alloc<RadixInitRec>(kRecTypeRadixInit, pgno);
        rec->id = id;
        rec->height = height;
        log(&rec->hdr, bytes);
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
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixErase(
    pgno_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    auto [rec, bytes] = alloc<RadixEraseRec>(kRecTypeRadixErase, pgno);
    rec->firstPos = (uint16_t) firstPos;
    rec->lastPos = (uint16_t) lastPos;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixPromote(pgno_t pgno, pgno_t refPage) {
    auto [rec, bytes] = alloc<RadixPromoteRec>(kRecTypeRadixPromote, pgno);
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixUpdate(
    pgno_t pgno,
    size_t refPos,
    pgno_t refPage
) {
    auto [rec, bytes] = alloc<RadixUpdateRec>(kRecTypeRadixUpdate, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}
