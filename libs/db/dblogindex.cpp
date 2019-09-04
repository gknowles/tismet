// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogindex.cpp - tismet db
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
struct IndexLeafInitRec {
    DbLog::Record hdr;
    uint32_t id;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLogRecInfo
*
***/

static DbLogRecInfo::Table s_radixRecInfo{
    { kRecTypeIndexLeafInit,
        DbLogRecInfo::sizeFn<IndexLeafInitRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<IndexLeafInitRec const &>(log);
            return notify->onLogApplyIndexLeafInit(page, rec.id);
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logIndexLeafInit(pgno_t pgno, uint32_t id) {
    auto [rec, bytes] = alloc<IndexLeafInitRec>(kRecTypeIndexLeafInit, pgno);
    rec->id = id;
    log(&rec->hdr, bytes);
}
