// Copyright Glen Knowles 2017 - 2022.
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
    kRecTypePageFree            = 5,  // [any]
    kRecTypeFullPage            = 16, // [any] id, data
    kRecTypeBitInit             = 17, // [bitmap] pos
    kRecTypeBitSet              = 38, // [bitmap] pos
    kRecTypeBitReset            = 39, // [bitmap] pos
    kRecTypeBitUpdateRange      = 6,  // [bitmap] firstPos, lastPos, value
    kRecTypeRadixInit           = 8,  // [radix] id, height
    kRecTypeRadixInitList       = 9,  // [radix] id, height, page list
    kRecTypeRadixErase          = 10, // [metric/radix] firstPos, lastPos
    kRecTypeRadixPromote        = 11, // [radix] refPage
    kRecTypeRadixUpdate         = 12, // [radix] refPos, refPage
    kRecTypeMetricInit          = 13, // [metric] name, id, retention, interval
    kRecTypeMetricUpdate        = 14, // [metric] retention, interval
    kRecTypeMetricClearSamples  = 15, // [metric] (clears index & last)
    kRecTypeMetricUpdatePos     = 32, // [metric] refPos, refTime
    kRecTypeMetricUpdatePosAndIndex = 33, // [metric] refPos, refTime, refPage
    kRecTypeMetricUpdateSample  = 34, // [metric] refSample
    kRecTypeMetricUpdateSampleAndIndex = 35, // [metric] refPos, refTime,
                                      // refSample, refPage
    // [metric] page, refSample (non-standard layout)
    kRecTypeMetricUpdateSampleTxn = 36,

    kRecTypeSampleInit          = 18, // [sample] id, stype, pageTime, lastPos
    kRecTypeSampleInitFill      = 37, // [sample] id, stype, pageTime, lastPos,
                                      //    value
    kRecTypeSampleUpdate        = 19, // [sample] first, last, value
                                      //    [first, last) = NANs, last = value
    kRecTypeSampleUpdateLast    = 20, // [sample] first, last, value
                                      //    [first, last) = NANs, last = value
                                      //    lastPos = last
    kRecTypeSampleUpdateTime    = 21, // [sample] pageTime
                                      //    pos = 0, samples[0] = NAN

    // [sample] page, pos, value (non-standard layout)
    kRecTypeSampleUpdateFloat32Txn      = 22,
    kRecTypeSampleUpdateFloat64Txn      = 24,
    kRecTypeSampleUpdateInt8Txn         = 26,
    kRecTypeSampleUpdateInt16Txn        = 28,
    kRecTypeSampleUpdateInt32Txn        = 30,

    // [sample] page, pos, value (non-standard layout)
    //    lastPos = pos
    kRecTypeSampleUpdateFloat32LastTxn  = 23,
    kRecTypeSampleUpdateFloat64LastTxn  = 25,
    kRecTypeSampleUpdateInt8LastTxn     = 27,
    kRecTypeSampleUpdateInt16LastTxn    = 29,
    kRecTypeSampleUpdateInt32LastTxn    = 31,

    kRecTypeUnused_7        = 7,   // Available

    kRecType_LastAvailable  = 40,
};

#pragma pack(push)
#pragma pack(1)

struct DbLog::Record {
    DbLogRecType type;
    pgno_t pgno;
    uint16_t localTxn;
};

#pragma pack(pop)

#define APPLY(name) static void apply ## name (const DbLogApplyArgs & args)

struct DbLogApplyArgs {
    DbLog::IApplyNotify * notify;
    void * page;
    const DbLog::Record * log;
    uint64_t lsn;
};
struct DbLogRecInfo {
    class Table;

    template<typename T>
    static uint16_t sizeFn(const DbLog::Record & log);

    static uint16_t defLocalTxnFn(const DbLog::Record & log) {
        return log.localTxn;
    }
    static pgno_t defPgnoFn(const DbLog::Record & log) {
        return log.pgno;
    }

    DbLogRecType m_type;

    uint16_t (*m_size)(const DbLog::Record & log);

    void (*m_apply)(const DbLogApplyArgs & args);

    uint16_t (*m_localTxn)(const DbLog::Record & log) = defLocalTxnFn;

    pgno_t (*m_pgno)(const DbLog::Record & log) = defPgnoFn;
};

class DbLogRecInfo::Table {
public:
    Table(std::initializer_list<DbLogRecInfo> list);
};

//===========================================================================
template<typename T>
uint16_t DbLogRecInfo::sizeFn(const DbLog::Record & log) {
    return sizeof(T);
}
