// Copyright Glen Knowles 2017 - 2023.
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

#pragma pack(push, 1)

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
*   DbWalRegisterRec
*
***/

static DbWalRecInfo s_codecs[kRecType_LastAvailable];

//===========================================================================
DbWalRegisterRec::DbWalRegisterRec(const DbWalRecInfo & info) {
    assert(info.m_type && info.m_type < size(s_codecs));
    assert(s_codecs[info.m_type].m_type == 0);
    s_codecs[info.m_type] = info;
}

//===========================================================================
DbWalRegisterRec::DbWalRegisterRec(initializer_list<DbWalRecInfo> infos) {
    for (auto && ri : infos)
        (void) DbWalRegisterRec(ri);
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
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
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
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
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
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
    return 0;
}

//===========================================================================
// static
uint64_t DbWal::getStartLsn(const Record & rec) {
    if (rec.type == kRecTypeCheckpoint)
        return reinterpret_cast<const CheckpointRec &>(rec).startLsn;
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
    return 0;
}

//===========================================================================
// static
void DbWal::setLocalTxn(DbWal::Record * rec, uint16_t localTxn) {
    rec->localTxn = localTxn;
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
uint64_t DbWal::walCheckpoint(uint64_t startLsn) {
    CheckpointRec rec;
    rec.type = kRecTypeCheckpoint;
    rec.startLsn = startLsn;
    auto lsn = wal((Record &) rec, sizeof(rec), TxnMode::kContinue);
    return lsn;
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
uint64_t DbWal::walCommitTxn(uint64_t txn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnCommit;
    rec.localTxn = getLocalTxn(txn);
    auto lsn = wal((Record &) rec, sizeof(rec), TxnMode::kCommit, txn);
    return getTxn(lsn, rec.localTxn);
}

//===========================================================================
void DbWal::walAndApply(uint64_t txn, Record * rec, size_t bytes) {
    assert(bytes >= sizeof(DbWal::Record));
    if (txn)
        rec->localTxn = getLocalTxn(txn);
    auto lsn = wal(*rec, bytes, TxnMode::kContinue);

    void * ptr = nullptr;
    auto pgno = getPgno(*rec);
    if (pgno == pgno_t::npos) {
        applyUpdate(ptr, lsn, *rec);
        return;
    }
    auto localTxn = getLocalTxn(*rec);
    ptr = m_page->onWalGetPtrForUpdate(pgno, lsn, localTxn);
    applyUpdate(ptr, lsn, *rec);
    m_page->onWalUnlockPtr(pgno);
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
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
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

static DbWalRegisterRec s_dataRecInfo = {
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
            args.notify->onWalApplyBeginTxn(
                args.lsn,
                localTxnTransaction(*args.rec)
            );
        },
        localTxnTransaction,
        invalidPgno,
    },
    { kRecTypeTxnCommit,
        DbWalRecInfo::sizeFn<TransactionRec>,
        [](auto args) {
            args.notify->onWalApplyCommitTxn(
                args.lsn,
                localTxnTransaction(*args.rec)
            );
        },
        localTxnTransaction,
        invalidPgno,
    },
};
