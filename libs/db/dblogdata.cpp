// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogdata.cpp - tismet db
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
// Checkpoint
struct CheckpointCommitRec {
    DbLogRecType type;
    uint64_t startLsn;
};

//---------------------------------------------------------------------------
// Transaction
struct TransactionRec {
    DbLogRecType type;
    uint16_t localTxn;
};

//---------------------------------------------------------------------------
struct ZeroUpdateRootsRec {
    DbLog::Record hdr;
    pgno_t infoRoot;
    pgno_t nameRoot;
    pgno_t idRoot;
};

//---------------------------------------------------------------------------
// Segment
struct SegmentUpdateRec {
    DbLog::Record hdr;
    pgno_t refPage;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLogRecInfo
*
***/

static DbLogRecInfo s_codecs[kRecType_LastAvailable];

//===========================================================================
DbLogRecInfo::Table::Table(initializer_list<DbLogRecInfo> list) {
    for (auto && ri : list) {
        assert(ri.m_type && ri.m_type < size(s_codecs));
        assert(s_codecs[ri.m_type].m_type == 0);
        s_codecs[ri.m_type] = ri;
    }
}


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
// static
uint16_t DbLog::getSize(Record const & log) {
    if (log.type && log.type < size(s_codecs)) {
        auto fn = s_codecs[log.type].m_size;
        if (fn)
            return fn(log);
    }
    logMsgFatal() << "Unknown log record type, " << log.type;
    return 0;
}

//===========================================================================
// static
pgno_t DbLog::getPgno(Record const & log) {
    if (log.type && log.type < size(s_codecs)) {
        auto fn = s_codecs[log.type].m_pgno;
        if (fn)
            return fn(log);
    }
    logMsgFatal() << "Unknown log record type, " << log.type;
    return {};
}

//===========================================================================
// static
uint16_t DbLog::getLocalTxn(Record const & log) {
    if (log.type && log.type < size(s_codecs)) {
        auto fn = s_codecs[log.type].m_localTxn;
        if (fn)
            return fn(log);
    }
    logMsgFatal() << "Unknown log record type, " << log.type;
    return 0;
}

namespace {

union LogPos {
    uint64_t txn;
    struct {
        uint64_t localTxn : 16;
        uint64_t lsn : 48;
    } u;
};

} // namespace

//===========================================================================
// static
uint64_t DbLog::getLsn(uint64_t logPos) {
    LogPos tmp;
    tmp.txn = logPos;
    return tmp.u.lsn;
}

//===========================================================================
// static
uint16_t DbLog::getLocalTxn(uint64_t logPos) {
    LogPos tmp;
    tmp.txn = logPos;
    return tmp.u.localTxn;
}

//===========================================================================
// static
uint64_t DbLog::getTxn(uint64_t lsn, uint16_t localTxn) {
    LogPos tmp;
    tmp.u.lsn = lsn;
    tmp.u.localTxn = localTxn;
    return tmp.txn;
}

//===========================================================================
// static
void DbLog::setLocalTxn(DbLog::Record * log, uint16_t localTxn) {
    log->localTxn = localTxn;
}

//===========================================================================
void DbLog::logCommitCheckpoint(uint64_t startLsn) {
    CheckpointCommitRec rec;
    rec.type = kRecTypeCommitCheckpoint;
    rec.startLsn = startLsn;
    log((Record &) rec, sizeof(rec), TxnMode::kContinue);
}

//===========================================================================
uint64_t DbLog::logBeginTxn(uint16_t localTxn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnBegin;
    rec.localTxn = localTxn;
    auto lsn = log((Record &) rec, sizeof(rec), TxnMode::kBegin);
    return getTxn(lsn, localTxn);
}

//===========================================================================
void DbLog::logCommit(uint64_t txn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnCommit;
    rec.localTxn = getLocalTxn(txn);
    log((Record &) rec, sizeof(rec), TxnMode::kCommit, txn);
}

//===========================================================================
void DbLog::logAndApply(uint64_t txn, Record * rec, size_t bytes) {
    assert(bytes >= sizeof(DbLog::Record));
    if (txn)
        rec->localTxn = getLocalTxn(txn);
    auto lsn = log(*rec, bytes, TxnMode::kContinue);
    apply(lsn, *rec);
}

//===========================================================================
void DbLog::apply(uint64_t lsn, Record const & log) {
    switch (log.type) {
    case kRecTypeCommitCheckpoint:
    case kRecTypeTxnBegin:
    case kRecTypeTxnCommit:
        return;
    default:
        break;
    }

    auto pgno = getPgno(log);
    auto localTxn = getLocalTxn(log);
    auto ptr = m_page->onLogGetUpdatePtr(pgno, lsn, localTxn);
    applyUpdate(ptr, log);
}

//===========================================================================
void DbLog::applyUpdate(void * page, Record const & log) {
    if (log.type && log.type < ::size(s_codecs)) {
        auto fn = s_codecs[log.type].m_apply;
        if (fn)
            return fn(m_data, page, log);
    }
    logMsgFatal() << "Unknown log record type, " << log.type;
}


/****************************************************************************
*
*   DbLog - recovery
*
***/

//===========================================================================
void DbLog::apply(AnalyzeData * data, uint64_t lsn, Record const & log) {
    switch (log.type) {
    case kRecTypeCommitCheckpoint: {
            auto & rec = reinterpret_cast<CheckpointCommitRec const &>(log);
            applyCommitCheckpoint(data, lsn, rec.startLsn);
        }
        break;
    case kRecTypeTxnBegin: {
            auto & rec = reinterpret_cast<TransactionRec const &>(log);
            applyBeginTxn(data, lsn, rec.localTxn);
        }
        break;
    case kRecTypeTxnCommit: {
            auto & rec = reinterpret_cast<TransactionRec const &>(log);
            applyCommitTxn(data, lsn, rec.localTxn);
        }
        break;
    default:
        applyUpdate(data, lsn, log);
        break;
    }
}


/****************************************************************************
*
*   DbLogRecInfo
*
***/

static DbLogRecInfo::Table s_dataRecInfo{
    { kRecTypeCommitCheckpoint,
        DbLogRecInfo::sizeFn<CheckpointCommitRec>,
        nullptr,
        nullptr,
        nullptr,
    },
    { kRecTypeTxnBegin,
        DbLogRecInfo::sizeFn<TransactionRec>,
        nullptr,
        [](auto & log) {
            return reinterpret_cast<TransactionRec const &>(log).localTxn; 
        },
        nullptr,
    },
    { kRecTypeTxnCommit,
        DbLogRecInfo::sizeFn<TransactionRec>,
        nullptr,
        [](auto & log) {
            return reinterpret_cast<TransactionRec const &>(log).localTxn; 
        },
        nullptr,
    },
    { kRecTypeZeroInit,
        DbLogRecInfo::sizeFn<DbLog::Record>,
        [](auto notify, void * page, auto & log) {
            notify->onLogApplyZeroInit(page); 
        },
    },
    { kRecTypeZeroUpdateRoots,
        DbLogRecInfo::sizeFn<ZeroUpdateRootsRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<ZeroUpdateRootsRec const &>(log);
            notify->onLogApplyZeroUpdateRoots(
                page, 
                rec.infoRoot,
                rec.nameRoot,
                rec.idRoot
            ); 
        },
    },
    { kRecTypePageFree,
        DbLogRecInfo::sizeFn<DbLog::Record>,
        [](auto notify, void * page, auto & log) {
            notify->onLogApplyPageFree(page); },
    },
    { kRecTypeSegmentAlloc,
        DbLogRecInfo::sizeFn<SegmentUpdateRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<SegmentUpdateRec const &>(log);
            return notify->onLogApplySegmentUpdate(page, rec.refPage, false);
        },
    },
    { kRecTypeSegmentFree,
        DbLogRecInfo::sizeFn<SegmentUpdateRec>,
        [](auto notify, void * page, auto & log) {
            auto & rec = reinterpret_cast<SegmentUpdateRec const &>(log);
            return notify->onLogApplySegmentUpdate(page, rec.refPage, true);
        },
    },
};


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
void DbTxn::logZeroInit(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeZeroInit, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logZeroUpdateRoots(
    pgno_t pgno,
    pgno_t infoRootPage,
    pgno_t nameRootPage,
    pgno_t idRootPage
) {
    
}

//===========================================================================
void DbTxn::logPageFree(pgno_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypePageFree, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logSegmentUpdate(
    pgno_t pgno,
    pgno_t refPage,
    bool free
) {
    auto [rec, bytes] = alloc<SegmentUpdateRec>(
        free ? kRecTypeSegmentFree : kRecTypeSegmentAlloc,
        pgno
    );
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}
