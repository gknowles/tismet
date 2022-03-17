// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// dbwalcodec.cpp - tismet db
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
struct CheckpointRec {
    DbWalRecType type;
    uint64_t startLsn;
};

//---------------------------------------------------------------------------
// Transaction
struct TransactionRec {
    DbWalRecType type;
    uint16_t localTxn;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbWalRecInfo
*
***/

static DbWalRecInfo s_codecs[kRecType_LastAvailable];

//===========================================================================
DbWalRecInfo::Table::Table(initializer_list<DbWalRecInfo> list) {
    for (auto && ri : list) {
        assert(ri.m_type && ri.m_type < size(s_codecs));
        assert(s_codecs[ri.m_type].m_type == 0);
        s_codecs[ri.m_type] = ri;
    }
}


/****************************************************************************
*
*   DbWal
*
***/

//===========================================================================
// static
uint16_t DbWal::getSize(const Record & rec) {
    if (rec.type && rec.type < size(s_codecs)) {
        auto fn = s_codecs[rec.type].m_size;
        if (fn)
            return fn(rec);
    }
    logMsgFatal() << "Unknown log record type, " << rec.type;
    return 0;
}

//===========================================================================
// static
pgno_t DbWal::getPgno(const Record & rec) {
    if (rec.type && rec.type < size(s_codecs)) {
        auto fn = s_codecs[rec.type].m_pgno;
        if (fn)
            return fn(rec);
    }
    logMsgFatal() << "Unknown log record type, " << rec.type;
    return {};
}

//===========================================================================
// static
uint16_t DbWal::getLocalTxn(const Record & rec) {
    if (rec.type && rec.type < size(s_codecs)) {
        auto fn = s_codecs[rec.type].m_localTxn;
        if (fn)
            return fn(rec);
    }
    logMsgFatal() << "Unknown log record type, " << rec.type;
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
uint64_t DbWal::getLsn(uint64_t walPos) {
    LogPos tmp;
    tmp.txn = walPos;
    return tmp.u.lsn;
}

//===========================================================================
// static
uint16_t DbWal::getLocalTxn(uint64_t walPos) {
    LogPos tmp;
    tmp.txn = walPos;
    return tmp.u.localTxn;
}

//===========================================================================
// static
uint64_t DbWal::getTxn(uint64_t lsn, uint16_t localTxn) {
    LogPos tmp;
    tmp.u.lsn = lsn;
    tmp.u.localTxn = localTxn;
    return tmp.txn;
}

//===========================================================================
// static
void DbWal::setLocalTxn(DbWal::Record * rec, uint16_t localTxn) {
    rec->localTxn = localTxn;
}

//===========================================================================
void DbWal::walCheckpoint(uint64_t startLsn) {
    CheckpointRec rec;
    rec.type = kRecTypeCheckpoint;
    rec.startLsn = startLsn;
    wal((Record &) rec, sizeof(rec), TxnMode::kContinue);
}

//===========================================================================
uint64_t DbWal::walBeginTxn(uint16_t localTxn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnBegin;
    rec.localTxn = localTxn;
    auto lsn = wal((Record &) rec, sizeof(rec), TxnMode::kBegin);
    return getTxn(lsn, localTxn);
}

//===========================================================================
void DbWal::walCommitTxn(uint64_t txn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnCommit;
    rec.localTxn = getLocalTxn(txn);
    wal((Record &) rec, sizeof(rec), TxnMode::kCommit, txn);
}

//===========================================================================
void DbWal::walAndApply(uint64_t txn, Record * rec, size_t bytes) {
    assert(bytes >= sizeof(DbWal::Record));
    if (txn)
        rec->localTxn = getLocalTxn(txn);
    auto lsn = wal(*rec, bytes, TxnMode::kContinue);

    void * ptr = nullptr;
    auto pgno = getPgno(*rec);
    if (pgno != pgno_t::npos) {
        auto localTxn = getLocalTxn(*rec);
        ptr = m_page->onWalGetUpdatePtr(pgno, lsn, localTxn);
    }
    applyUpdate(ptr, lsn, *rec);
}

//===========================================================================
void DbWal::applyUpdate(void * page, uint64_t lsn, const Record & rec) {
    if (rec.type && rec.type < ::size(s_codecs)) {
        auto * fn = s_codecs[rec.type].m_apply;
        if (fn) {
            DbWalApplyArgs args = {
                .notify = m_data,
                .page = page,
                .rec = &rec,
                .lsn = lsn
            };
            return fn(args);
        }
    }
    logMsgFatal() << "Unknown log record type, " << rec.type;
}


/****************************************************************************
*
*   DbWal - recovery
*
***/

//===========================================================================
void DbWal::apply(AnalyzeData * data, uint64_t lsn, const Record & raw) {
    switch (raw.type) {
    case kRecTypeCheckpoint: {
            auto & rec = reinterpret_cast<const CheckpointRec &>(raw);
            applyCheckpoint(data, lsn, rec.startLsn);
        }
        break;
    case kRecTypeTxnBegin: {
            auto & rec = reinterpret_cast<const TransactionRec &>(raw);
            applyBeginTxn(data, lsn, rec.localTxn);
        }
        break;
    case kRecTypeTxnCommit: {
            auto & rec = reinterpret_cast<const TransactionRec &>(raw);
            applyCommitTxn(data, lsn, rec.localTxn);
        }
        break;
    default:
        applyUpdate(data, lsn, raw);
        break;
    }
}


/****************************************************************************
*
*   DbWalRecInfo
*
***/

//===========================================================================
static uint16_t localTxnTransaction(const DbWal::Record & raw) {
    return reinterpret_cast<const TransactionRec &>(raw).localTxn;
}

//===========================================================================
static pgno_t invalidPgno(const DbWal::Record & raw) {
    return pgno_t::npos;
}

static DbWalRecInfo::Table s_dataRecInfo = {
    { kRecTypeCheckpoint,
        DbWalRecInfo::sizeFn<CheckpointRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const CheckpointRec *>(args.rec);
            args.notify->onWalApplyCheckpoint(args.lsn, rec->startLsn);
        },
        nullptr,    // localTxn
        invalidPgno,
    },
    { kRecTypeTxnBegin,
        DbWalRecInfo::sizeFn<TransactionRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const TransactionRec *>(args.rec);
            args.notify->onWalApplyBeginTxn(args.lsn, rec->localTxn);
        },
        localTxnTransaction,
        invalidPgno,
    },
    { kRecTypeTxnCommit,
        DbWalRecInfo::sizeFn<TransactionRec>,
        [](auto args) {
            auto rec = reinterpret_cast<const TransactionRec *>(args.rec);
            args.notify->onWalApplyCommitTxn(args.lsn, rec->localTxn);
        },
        localTxnTransaction,
        invalidPgno,
    },
};
