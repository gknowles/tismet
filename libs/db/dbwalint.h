// Copyright Glen Knowles 2017 - 2025.
// Distributed under the Boost Software License, Version 1.0.
//
// dbwalint.h - tismet db
#pragma once


/****************************************************************************
*
*   DbWal::Record
*
***/

enum DbWalRecType : int8_t {
    kRecTypeCheckpoint          = 1,    // [N/A] startLsn
    kRecTypeTxnBegin            = 2,    // [N/A]
    kRecTypeTxnCommit           = 3,    // [N/A]
    kRecTypeTxnGroupCommit      = 4,    // [N/A] numTxns, txns

    kRecTypeZeroInit            = 5,    // [master]
    kRecTypeRootUpdate          = 6,    // [master] rootPage
    kRecTypePageFree            = 7,    // [any]
    kRecTypeFullPage            = 8,    // [any] id, data
    kRecTypeBitInit             = 9,    // [bitmap] pos
    kRecTypeBitSet              = 10,   // [bitmap] pos
    kRecTypeBitReset            = 11,   // [bitmap] pos
    kRecTypeBitUpdateRange      = 12,   // [bitmap] firstPos, lastPos, value
    kRecTypeRadixInit           = 13,   // [radix] id, height
    kRecTypeRadixInitList       = 14,   // [radix] id, height, page list
    kRecTypeRadixErase          = 15,   // [radix] firstPos, lastPos
    kRecTypeRadixPromote        = 16,   // [radix] refPage
    kRecTypeRadixUpdate         = 17,   // [radix] refPos, refPage

    kRecTypeSampleInit          = 18,   // [sample] id, stype, time, value
    kRecTypeSampleUpdateRoot    = 19,   // [sample] rootPage
    kRecTypeSampleUpdateTime    = 20,   // [sample] firstTime, lastTime
    kRecTypeSampleUpdateFirstTime = 21, // [sample] pageTime
    kRecTypeSampleUpdateLastTime = 22,  // [sample] pageTime
    kRecTypeSampleInsert        = 23,   // [sample]
    kRecTypeSampleInsertBack    = 24,   // [sample]
    kRecTypeSampleInsertFront   = 25,   // [sample]
    kRecTypeSampleErase         = 26,   // [sample]
    kRecTypeSampleEraseFront    = 27,   // [sample]
    kRecTypeSampleEraseBack     = 28,   // [sample]
    kRecTypeSampleReplace       = 29,   // [sample]
    kRecTypeSampleReplaceFront  = 30,   // [sample]
    kRecTypeSampleReplaceBack   = 31,   // [sample]

    kRecType_LastAvailable  = 32,
};

#pragma pack(push, 1)

struct DbWal::Record {
    DbWalRecType type;
    pgno_t pgno;
    LocalTxn localTxn;
};

#pragma pack(pop)

struct DbWalApplyArgs {
    DbWal::IApplyNotify * notify;
    void * page;
    const DbWal::Record * rec;
    Lsn lsn;
};
struct DbWalRecInfo {
    template<typename T>
    static uint16_t sizeFn(const DbWal::Record & rec) {
        return sizeof(T);
    }
    static LocalTxn defLocalTxnFn(const DbWal::Record & rec) {
        return rec.localTxn;
    }
    static pgno_t defPgnoFn(const DbWal::Record & rec) {
        return rec.pgno;
    }

    DbWalRecType m_type;
    uint16_t (*m_size)(const DbWal::Record & rec);
    void (*m_apply)(const DbWalApplyArgs & args);
    LocalTxn (*m_localTxn)(const DbWal::Record & rec) = defLocalTxnFn;
    pgno_t (*m_pgno)(const DbWal::Record & rec) = defPgnoFn;
};

class DbWalRegisterRec {
public:
    explicit DbWalRegisterRec(const DbWalRecInfo & info);
    DbWalRegisterRec(std::initializer_list<DbWalRecInfo> infos);
};
