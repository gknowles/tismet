// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdataindex.cpp - tismet db
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
*   Variables
*
***/


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   B-tree index
*
***/

//===========================================================================
// static
string DbData::toKey(TimePoint key) {
    assert(sizeof(key.time_since_epoch().count()) == 8);
    string out(8, '\0');
    hton64(out.data(), key.time_since_epoch().count());
    return out;
}

//===========================================================================
// static
string DbData::toKey(uint32_t key) {
    string out(4, '\0');
    hton32(out.data(), key);
    return out;
}

//===========================================================================
// static
bool DbData::fromKey(uint32_t * out, std::string_view src) {
    if (src.size() == sizeof(*out)) {
        *out = ntoh32(src.data());
        return true;
    }
    return false;
}

//===========================================================================
// static
bool DbData::fromKey(pgno_t * out, std::string_view src) {
    return fromKey((underlying_type_t<pgno_t> *) out, src);
}

//===========================================================================
void DbData::onLogApplyIndexLeafInit(void * ptr, uint32_t id) {
    auto ip = static_cast<IndexPage *>(ptr);
    if (ip->hdr.type == DbPageType::kFree) {
        memset((char *) ip + sizeof(ip->hdr), 0, m_pageSize - sizeof(ip->hdr));
    } else {
        assert(ip->hdr.type == DbPageType::kInvalid);
    }
    ip->hdr.type = DbPageType::kIndexLeaf;
    ip->hdr.id = id;
}

//===========================================================================
void DbData::indexDestructPage(DbTxn & txn, pgno_t pgno) {
    assert(!"Not implemented");
}

//===========================================================================
void DbData::indexDestruct(DbTxn & txn, const DbPageHeader & hdr) {
    assert(!"Not implemented");
}

//===========================================================================
bool DbData::indexInsertLeafPgno(
    DbTxn & txn,
    pgno_t root,
    std::string_view key,
    pgno_t pgno
) {
    assert(!"Not implemented");
    return false;
}

//===========================================================================
bool DbData::indexEraseLeafPgno(
    DbTxn & txn,
    pgno_t root,
    std::string_view key,
    pgno_t pgno
) {
    assert(!"Not implemented");
    return false;
}

//===========================================================================
bool DbData::indexFindLeafPgno(
    const DbTxn & txn,
    pgno_t * out,
    pgno_t root,
    std::string_view key,
    bool after
) {
    assert(!"Not implemented");
    return false;
}

//===========================================================================
bool DbData::indexUpdate(
    DbTxn & txn,
    pgno_t root,
    std::string_view key,
    std::string_view data
) {
    assert(!"Not implemented");
    return false;
}

//===========================================================================
void DbData::indexErase(DbTxn & txn, pgno_t root, std::string_view key) {
    assert(!"Not implemented");
}

//===========================================================================
bool DbData::indexFind(
    const DbTxn & txn,
    uint32_t * out,
    pgno_t root,
    std::string_view key
) const {
    assert(!"Not implemented");
    return false;
}

//===========================================================================
bool DbData::indexFind(
    const DbTxn & txn,
    std::string * out,
    pgno_t root,
    std::string_view key
) const {
    assert(!"Not implemented");
    return false;
}
