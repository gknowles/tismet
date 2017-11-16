// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dblog.cpp - tismet db
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

struct DbLog::Record {
    uint64_t seq : 48;
    uint64_t txn : 16;
    uint32_t pgno;
    DbLogRecType type;
};

namespace {

struct SegmentUpdateRec {
    DbLog::Record hdr;
    uint32_t pgno;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
DbLog::DbLog(DbData & data)
    : m_data(data)
{}

//===========================================================================
uint16_t DbLog::beginTrans() {
    for (;;) {
        if (++m_lastTxnId)
            break;
    }
    return m_lastTxnId;
}

//===========================================================================
void DbLog::commit(uint16_t txn) {
}

//===========================================================================
template<typename T>
T * DbLog::alloc(
    uint16_t txn,
    DbLogRecType type,
    uint32_t pgno,
    size_t bytes
) {
    assert(bytes >= sizeof(T));
    return (T *) alloc(txn, type, pgno, bytes);
}

//===========================================================================
DbLog::Record * DbLog::alloc(
    uint16_t txn,
    DbLogRecType type,
    uint32_t pgno,
    size_t bytes
) {
    static string s_rec;
    assert(txn);
    auto lsn = ++m_lastLsn;
    assert(bytes >= sizeof(DbLog::Record));
    s_rec.resize(bytes);
    auto rec = (Record *) s_rec.data();
    rec->txn = txn;
    rec->seq = lsn;
    rec->type = type;
    rec->pgno = pgno;
    return rec;
}

//===========================================================================
void DbLog::apply(Record * hdr) {
    switch (hdr->type) {
    case kRecTypePageFree:
        return m_data.applyPageFree(hdr->pgno);
    case kRecTypeSegmentInit:
        return m_data.applySegmentInit(hdr->pgno);
    case kRecTypeSegmentAlloc: {
        auto rec = reinterpret_cast<SegmentUpdateRec *>(hdr);
        return m_data.applySegmentUpdate(hdr->pgno, rec->pgno, false);
    }
    case kRecTypeSegmentFree: {
        auto rec = reinterpret_cast<SegmentUpdateRec *>(hdr);
        return m_data.applySegmentUpdate(hdr->pgno, rec->pgno, true);
    }
    case kRecTypeRadixInit:
    case kRecTypeRadixInitList:
    case kRecTypeRadixUpdate:
    case kRecTypeRadixErase:
    case kRecTypeMetricInit:
    case kRecTypeMetricUpdate:
    case kRecTypeMetricClearIndex:
    case kRecTypeMetricUpdateIndex:
    case kRecTypeMetricUpdateLast:
    case kRecTypeMetricUpdateIndexAndLast:
    case kRecTypeSampleInit:
    case kRecTypeSampleUpdate:
    case kRecTypeSampleUpdateLast:
    case kRecTypeSampleUpdateTime:
    case kRecTypeTxnCommit:
        abort();
    }
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbLog & log)
    : m_log{log}
{
    m_txn = m_log.beginTrans();
}

//===========================================================================
DbTxn::~DbTxn() {
    m_log.commit(m_txn);
}

//===========================================================================
void DbTxn::logPageFree(uint32_t pgno) {
    auto rec = m_log.alloc<DbLog::Record>(
        m_txn,
        kRecTypePageFree,
        pgno
    );
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logSegmentInit(uint32_t pgno) {
    auto rec = m_log.alloc<DbLog::Record>(
        m_txn,
        kRecTypeSegmentInit,
        pgno
    );
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logSegmentUpdate(
    uint32_t pgno,
    uint32_t refPage,
    bool free
) {
    auto rec = m_log.alloc<SegmentUpdateRec>(
        m_txn,
        free ? kRecTypeSegmentFree : kRecTypeSegmentAlloc,
        pgno
    );
    rec->pgno = refPage;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logRadixInit(
    uint32_t pgno,
    uint16_t height,
    uint32_t * firstPage,
    uint32_t * lastPage
) {
}

//===========================================================================
void DbTxn::logRadixUpdate(
    uint32_t pgno,
    size_t pos,
    uint32_t refPage
) {
}

//===========================================================================
void DbTxn::logRadixErase(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos
) {
}

//===========================================================================
void DbTxn::logMetricInit(
    uint32_t pgno,
    string_view name,
    Duration retention,
    Duration interval
) {
}

//===========================================================================
void DbTxn::logMetricUpdate(
    uint32_t pgno,
    Duration retention,
    Duration interval
) {
}

//===========================================================================
void DbTxn::logMetricClearSamples(uint32_t pgno) {
}

//===========================================================================
void DbTxn::logMetricUpdateSamples(
    uint32_t pgno,
    size_t pos,
    uint32_t refPage,
    bool updateLast,
    bool updateIndex
) {
}

//===========================================================================
void DbTxn::logSampleInit(
    uint32_t pgno,
    TimePoint pageTime,
    size_t lastPos
) {
}

//===========================================================================
void DbTxn::logSampleUpdate(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos,
    float value,
    bool updateLast
) {
}

//===========================================================================
void DbTxn::logSampleUpdateTime(
    uint32_t pgno,
    TimePoint pageTime
) {
}
