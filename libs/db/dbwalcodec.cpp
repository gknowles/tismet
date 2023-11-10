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
    Lsn startLsn;
};

//---------------------------------------------------------------------------
// Transaction
struct TransactionRec {
    DbWalRecType type;
    LocalTxn localTxn;
};
struct TransactionGroupRec {
    DbWalRecType type;
    uint8_t numTxns;

    // EXTENDS BEYOND END OF STRUCT
    LocalTxn txns[1];
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
LocalTxn DbWal::getLocalTxn(const Record & rec) {
    if (rec.type && rec.type < size(s_codecs)) {
        auto fn = s_codecs[rec.type].m_localTxn;
        if (fn)
            return fn(rec);
    }
    logMsgFatal() << "Unknown WAL record type, " << rec.type;
    return {};
}

//===========================================================================
// static
vector<LocalTxn> DbWal::getLocalTxns(const Record & raw) {
    vector<LocalTxn> out;
    if (raw.type != kRecTypeTxnGroupCommit) {
        logMsgFatal() << "WAL record type doesn't have txn list, "
            << raw.type;
        return out;
    }
    auto & rec = reinterpret_cast<const TransactionGroupRec &>(raw);
    out.resize(rec.numTxns);
    memcpy(out.data(), rec.txns, rec.numTxns * sizeof *rec.txns);
    return out;
}

//===========================================================================
// static
Lsn DbWal::getStartLsn(const Record & rec) {
    if (rec.type != kRecTypeCheckpoint) {
        logMsgFatal() << "WAL record type doesn't have start LSN, "
            << rec.type;
        return {};
    }
    return reinterpret_cast<const CheckpointRec &>(rec).startLsn;
}

//===========================================================================
// static
void DbWal::setLocalTxn(DbWal::Record * rec, LocalTxn localTxn) {
    rec->localTxn = localTxn;
}

//===========================================================================
// static
Lsn DbWal::getLsn(Lsx walPos) {
    return {walPos.lsn};
}

//===========================================================================
// static
LocalTxn DbWal::getLocalTxn(Lsx walPos) {
    return (LocalTxn) walPos.localTxn;
}

//===========================================================================
// static
Lsx DbWal::getTxn(Lsn lsn, LocalTxn localTxn) {
    Lsx tmp;
    tmp.lsn = lsn.val;
    tmp.localTxn = localTxn;
    return tmp;
}

//===========================================================================
Lsn DbWal::walCheckpoint(Lsn startLsn) {
    CheckpointRec rec;
    rec.type = kRecTypeCheckpoint;
    rec.startLsn = startLsn;
    auto lsn = wal((Record &) rec, sizeof(rec), TxnMode::kContinue);
    return lsn;
}

//===========================================================================
Lsx DbWal::walBeginTxn(LocalTxn localTxn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnBegin;
    rec.localTxn = localTxn;
    auto lsn = wal((Record &) rec, sizeof(rec), TxnMode::kBegin);
    return getTxn(lsn, localTxn);
}

//===========================================================================
void DbWal::walCommitTxn(Lsx txn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnCommit;
    rec.localTxn = getLocalTxn(txn);
    wal((Record &) rec, sizeof(rec), TxnMode::kCommit, txn);
}

//===========================================================================
void DbWal::walCommitTxns(const unordered_set<Lsx> & txns) {
    auto num = txns.size();
    if (num == 1)
        return walCommitTxn(*txns.begin());

    assert(num > 1);
    auto extra = num * sizeof uint16_t;
    auto offset = offsetof(TransactionGroupRec, txns);
    string buf;
    buf.resize(offset + extra);
    auto * lr = (TransactionGroupRec *) buf.data();
    lr->type = kRecTypeTxnGroupCommit;
    assert(num < numeric_limits<uint8_t>::max());
    lr->numTxns = (uint8_t) num;
    auto ptr = lr->txns;
    for (auto&& txn : txns)
        *ptr++ = getLocalTxn(txn);
    wal(*(Record *) lr, buf.size(), TxnMode::kCommit, {}, &txns);
}

//===========================================================================
void DbWal::walAndApply(Lsx txn, Record * rec, size_t bytes) {
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
void DbWal::applyUpdate(void * page, Lsn lsn, const Record & rec) {
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
static LocalTxn localTxnTransaction(const DbWal::Record & raw) {
    return reinterpret_cast<const TransactionRec &>(raw).localTxn;
}

//===========================================================================
static pgno_t invalidPgno(const DbWal::Record & raw) {
    return pgno_t::npos;
}

//===========================================================================
static uint16_t sizeGroupCommit(const DbWal::Record & raw) {
    auto & rec = reinterpret_cast<const TransactionGroupRec &>(raw);
    return offsetof(TransactionGroupRec, txns)
        + rec.numTxns * sizeof *rec.txns;
}

//===========================================================================
static void applyGroupCommit(const DbWalApplyArgs & args) {
    auto rec = reinterpret_cast<const TransactionGroupRec *>(args.rec);
    vector<LocalTxn> txns(rec->numTxns);
    memcpy(txns.data(), rec->txns, rec->numTxns * sizeof *rec->txns);
    args.notify->onWalApplyGroupCommitTxn(args.lsn, txns);
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
    { kRecTypeTxnGroupCommit,
        sizeGroupCommit,
        applyGroupCommit,
        nullptr,    // localTxn
        invalidPgno,
    },
};
