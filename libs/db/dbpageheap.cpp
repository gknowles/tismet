// Copyright Glen Knowles 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbpageheap.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   DbPageHeap
*
***/

//===========================================================================
DbPageHeap::DbPageHeap(DbTxn & txn, DbData & data)
    : m_data(data)
    , m_txn(txn)
{}

//===========================================================================
size_t DbPageHeap::create() {
    return m_data.allocPgno(m_txn);
}

//===========================================================================
void DbPageHeap::destroy(size_t pgno) {
    m_data.deprecatePage(m_txn, (pgno_t) pgno);
}

//===========================================================================
void DbPageHeap::setRoot(size_t pgno) {
    m_txn.walTagRootUpdate((pgno_t) 0, (pgno_t) pgno);
}

//===========================================================================
size_t DbPageHeap::root() const {
    return m_data.m_metricTagStoreRoot;
}

//===========================================================================
size_t DbPageHeap::pageSize() const {
    return m_txn.pageSize() - sizeof DbPageHeader;
}

//===========================================================================
bool DbPageHeap::empty() const {
    return m_data.m_metricTagStoreRoot == pgno_t::npos;
}

//===========================================================================
bool DbPageHeap::empty(size_t pgno) const {
    assert(!"Testing for existance of specific trie page not supported.");
    return false;
}

//===========================================================================
uint8_t * DbPageHeap::wptr(size_t pgno) {
    commitIfPending();
    auto psize = m_txn.pageSize();
    m_updatePgno = (pgno_t) pgno;
    auto pc = m_txn.allocFullPage(m_updatePgno, psize);
    m_updatePtr = reinterpret_cast<uint8_t *>(pc.first);
    auto offset = pc.second - psize + sizeof DbPageHeader;
    return m_updatePtr + offset;
}

//===========================================================================
const uint8_t * DbPageHeap::ptr(size_t pgno) const {
    commitIfPending();
    auto page = m_txn.viewPage<DbPageHeader>((pgno_t) pgno);
    return reinterpret_cast<const uint8_t *>(page);
}

//===========================================================================
void DbPageHeap::commitIfPending() const {
    if (m_updatePtr) {
        assert(m_updatePgno != pgno_t::npos);
        m_txn.walFullPageInit(DbPageType::kTrie, 0, m_txn.pageSize());
        m_updatePtr = nullptr;
    }
}


/****************************************************************************
*
*   DbWalRecInfo
*
***/

#pragma pack(push)
#pragma pack(1)

namespace {

struct FullPageInitRec {
    DbWal::Record hdr;
    DbPageType type;
    uint32_t id;
    uint16_t dataLen;

    // EXTENDS BEYOND END OF STRUCT
    uint8_t data[1];
};

} // namespace

#pragma pack(pop)


static DbWalRegisterRec s_dataRecInfo = {
    { kRecTypeFullPage,
        [](auto & raw) -> uint16_t {    // size
            auto & rec = reinterpret_cast<const FullPageInitRec &>(raw);
            return offsetof(FullPageInitRec, data) + rec.dataLen;
        },
        [](auto args) {
            auto rec = reinterpret_cast<const FullPageInitRec *>(args.rec);
            args.notify->onWalApplyFullPageInit(
                args.page,
                rec->type,
                rec->id,
                {rec->data, rec->dataLen}
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
std::pair<void *, size_t> DbTxn::allocFullPage(pgno_t pgno, size_t extra) {
    assert(extra <= pageSize());
    auto offset = offsetof(FullPageInitRec, data);
    return alloc(kRecTypeFullPage, pgno, offset + extra);
}

//===========================================================================
void DbTxn::walFullPageInit(DbPageType type, uint32_t id, size_t extra) {
    assert(extra <= pageSize());
    auto offset = offsetof(FullPageInitRec, data);
    auto rec = reinterpret_cast<FullPageInitRec *>(m_buffer.data());
    assert(rec->hdr.type == kRecTypeFullPage);
    rec->type = type;
    rec->id = id;
    rec->dataLen = (uint16_t) extra;
    wal(&rec->hdr, offset + extra);
}

//===========================================================================
void DbTxn::walFullPageInit(
    pgno_t pgno, 
    DbPageType type, 
    uint32_t id, 
    std::span<uint8_t> data
) {
    auto extra = data.size();
    auto offset = offsetof(FullPageInitRec, data);
    assert(extra <= pageSize());
    auto [rec, bytes] = alloc<FullPageInitRec>(
        kRecTypeFullPage,
        pgno,
        offset + extra
    );
    rec->type = type;
    rec->id = id;
    rec->dataLen = (uint16_t) extra;
    memcpy(rec->data, data.data(), extra);
    wal(&rec->hdr, bytes);
}


/****************************************************************************
*
*   Log apply
*
***/

//===========================================================================
void DbData::onWalApplyFullPageInit(
    void * ptr,
    DbPageType type,
    uint32_t id,
    std::span<const uint8_t> data
) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    auto offset = sizeof *hdr + data.size();
    assert(offset <= m_pageSize);
    if (hdr->type == DbPageType::kFree) {
        memset((char *) hdr + offset, 0, m_pageSize - offset);
    } else {
        assert(hdr->type == DbPageType::kInvalid);
    }
    hdr->type = type;
    hdr->id = id;
    memcpy(hdr + 1, data.data(), data.size());
}
