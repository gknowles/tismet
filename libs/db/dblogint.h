// Copyright Glen Knowles 2017 - 2019.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogint.h - tismet db
#pragma once


/****************************************************************************
*
*   DbLog::Record
*
***/

enum DbLogRecType : int8_t {
    kRecTypeCommitCheckpoint    = 1,  // [N/A] startLsn
    kRecTypeTxnBegin            = 2,  // [N/A]
    kRecTypeTxnCommit           = 3,  // [N/A]

    kRecTypeZeroInit            = 4,  // [master]
    kRecTypeZeroUpdateRoots     = 5,  // [master] infoRoot, nameRoot, idRoot
    kRecTypePageFree            = 6,  // [any]
    kRecTypeSegmentAlloc        = 7,  // [master/segment] refPage
    kRecTypeSegmentFree         = 8,  // [master/segment] refPage
    kRecTypeRadixInit           = 9,  // [radix] id, height
    kRecTypeRadixInitList       = 10, // [radix] id, height, page list
    kRecTypeRadixErase          = 11, // [metric/radix] firstPos, lastPos
    kRecTypeRadixPromote        = 12, // [radix] refPage
    kRecTypeRadixUpdate         = 13, // [radix] refPos, refPage
    kRecTypeMetricInit          = 14, // [metric] name, id, retention, interval
    kRecTypeMetricUpdate        = 15, // [metric] retention, interval
    kRecTypeMetricEraseSamples  = 16, // [metric] (clears index & last)
    kRecTypeMetricUpdateSample  = 17, // [metric] refSample
    kRecTypeMetricInsertSample  = 18, // [metric] refPos, refTime,
                                      // refSample, refPage
    // [metric] page, refSample (non-standard layout)
    kRecTypeMetricInsertSampleTxn = 19,

    kRecTypeSampleInit          = 20, // [sample] id, stype, pageTime, lastPos
    kRecTypeSampleBulkUpdate    = 21, // [sample] first, last, value
                                      //    [first, last) = NANs, last = value
    kRecTypeIndexLeafInit       = 22, // [index] id

    kRecType_LastAvailable  = 23,
};

#pragma pack(push)
#pragma pack(1)

struct DbLog::Record {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t localTxn;
};

#pragma pack(pop)

#define APPLY(name) static void apply ## name ( \
    DbLog::IApplyNotify * notify, \
    void * page, \
    DbLog::Record const & log)

struct DbLogRecInfo {
    class Table;

    template<typename T>
    static uint16_t sizeFn(DbLog::Record const & log);

    static uint16_t defLocalTxnFn(DbLog::Record const & log) {
        return log.localTxn;
    }
    static pgno_t defPgnoFn(DbLog::Record const & log) {
        return log.pgno;
    }

    DbLogRecType m_type;

    uint16_t (*m_size)(DbLog::Record const & log);

    void (*m_apply)(
        DbLog::IApplyNotify * notify,
        void * page,
        DbLog::Record const & log
    );

    uint16_t (*m_localTxn)(DbLog::Record const & log) = defLocalTxnFn;

    pgno_t (*m_pgno)(DbLog::Record const & log) = defPgnoFn;
};

class DbLogRecInfo::Table {
public:
    Table(std::initializer_list<DbLogRecInfo> list);
};

//===========================================================================
template<typename T>
uint16_t DbLogRecInfo::sizeFn(DbLog::Record const & log) {
    return sizeof(T);
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
template<typename T>
std::pair<T *, size_t> DbTxn::alloc(
    DbLogRecType type,
    pgno_t pgno,
    size_t bytes
) {
    assert(bytes >= sizeof(T));
    if (!m_txn)
        m_txn = m_log.beginTxn();
    m_buffer.resize(bytes);
    auto * lr = (DbLog::Record *) m_buffer.data();
    lr->type = type;
    lr->pgno = pgno;
    lr->localTxn = 0;
    return {(T *) m_buffer.data(), bytes};
}
