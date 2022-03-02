// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogcodec.cpp - tismet db
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
uint16_t DbLog::getSize(const Record & log) {
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
pgno_t DbLog::getPgno(const Record & log) {
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
uint16_t DbLog::getLocalTxn(const Record & log) {
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

    void * ptr = nullptr;
    auto pgno = getPgno(*rec);
    if (pgno != pgno_t::npos) {
        auto localTxn = getLocalTxn(*rec);
        ptr = m_page->onLogGetUpdatePtr(pgno, lsn, localTxn);
    }
    applyUpdate(ptr, lsn, *rec);
}

//===========================================================================
void DbLog::applyUpdate(void * page, uint64_t lsn, const Record & log) {
    if (log.type && log.type < ::size(s_codecs)) {
        auto * fn = s_codecs[log.type].m_apply;
        if (fn) {
            DbLogApplyArgs args = {
                .notify = m_data,
                .page = page,
                .log = &log,
                .lsn = lsn
            };
            return fn(args);
        }
    }
    logMsgFatal() << "Unknown log record type, " << log.type;
}


/****************************************************************************
*
*   DbLog - recovery
*
***/

//===========================================================================
void DbLog::apply(AnalyzeData * data, uint64_t lsn, const Record & log) {
    switch (log.type) {
    case kRecTypeCommitCheckpoint: {
            auto & rec = reinterpret_cast<const CheckpointCommitRec &>(log);
            applyCommitCheckpoint(data, lsn, rec.startLsn);
        }
        break;
    case kRecTypeTxnBegin: {
            auto & rec = reinterpret_cast<const TransactionRec &>(log);
            applyBeginTxn(data, lsn, rec.localTxn);
        }
        break;
    case kRecTypeTxnCommit: {
            auto & rec = reinterpret_cast<const TransactionRec &>(log);
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

//===========================================================================
static uint16_t localTxnTransaction(const DbLog::Record & log) {
    return reinterpret_cast<const TransactionRec &>(log).localTxn;
}

//===========================================================================
static pgno_t invalidPgno(const DbLog::Record & log) {
    return pgno_t::npos;
}

static DbLogRecInfo::Table s_dataRecInfo = {
    { kRecTypeCommitCheckpoint,
        DbLogRecInfo::sizeFn<CheckpointCommitRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const CheckpointCommitRec *>(args.log);
            args.notify->onLogApplyCommitCheckpoint(args.lsn, rec->startLsn);
        },
        nullptr,    // localTxn
        invalidPgno,
    },
    { kRecTypeTxnBegin,
        DbLogRecInfo::sizeFn<TransactionRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const TransactionRec *>(args.log);
            args.notify->onLogApplyBeginTxn(args.lsn, rec->localTxn);
        },
        localTxnTransaction,
        invalidPgno,
    },
    { kRecTypeTxnCommit,
        DbLogRecInfo::sizeFn<TransactionRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const TransactionRec *>(args.log);
            args.notify->onLogApplyCommitTxn(args.lsn, rec->localTxn);
        },
        localTxnTransaction,
        invalidPgno,
    },
};
