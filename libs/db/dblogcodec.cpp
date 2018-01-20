// Copyright Glen Knowles 2017.
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

enum DbLogRecType : int8_t {
    kRecTypeCommitCheckpoint    = 1,  // [N/A] startLsn
    kRecTypeTxnBegin            = 2,  // [N/A]
    kRecTypeTxnCommit           = 3,  // [N/A]

    kRecTypeZeroInit            = 4,  // [master]
    kRecTypePageFree            = 5,  // [any]
    kRecTypeSegmentAlloc        = 6,  // [master/segment] refPage
    kRecTypeSegmentFree         = 7,  // [master/segment] refPage
    kRecTypeRadixInit           = 8,  // [radix] id, height
    kRecTypeRadixInitList       = 9,  // [radix] id, height, page list
    kRecTypeRadixErase          = 10, // [metric/radix] firstPos, lastPos
    kRecTypeRadixPromote        = 11, // [radix] refPage
    kRecTypeRadixUpdate         = 12, // [radix] refPos, refPage
    kRecTypeMetricInit          = 13, // [metric] name, id, retention, interval
    kRecTypeMetricUpdate        = 14, // [metric] retention, interval
    kRecTypeMetricClearSamples  = 15, // [metric] (clears index & last)
    kRecTypeMetricUpdateLast    = 16, // [metric] refPos, refPage
    kRecTypeMetricUpdateLastAndIndex = 17, // [metric] refPos, refPage
    kRecTypeSampleInit          = 18, // [sample] id, pageTime, lastPos
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

    kRecType_NextAvailable = 32,
};

#pragma pack(push)
#pragma pack(1)

struct DbLog::Record {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t localTxn;
};

namespace {

union LogPos {
    uint64_t txn;
    struct {
        uint64_t localTxn : 16;
        uint64_t lsn : 48;
    } u;
};

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
// Segment
struct SegmentUpdateRec {
    DbLog::Record hdr;
    uint32_t refPage;
};

//---------------------------------------------------------------------------
// Radix
struct RadixInitRec {
    DbLog::Record hdr;
    uint32_t id;
    uint16_t height;
};
struct RadixInitListRec {
    DbLog::Record hdr;
    uint32_t id;
    uint16_t height;
    uint16_t numPages;

    // EXTENDS BEYOND END OF STRUCT
    uint32_t pages[1];
};
struct RadixEraseRec {
    DbLog::Record hdr;
    uint16_t firstPos;
    uint16_t lastPos;
};
struct RadixPromoteRec {
    DbLog::Record hdr;
    uint32_t refPage;
};
struct RadixUpdateRec {
    DbLog::Record hdr;
    uint16_t refPos;
    uint32_t refPage;
};

//---------------------------------------------------------------------------
// Metric
struct MetricInitRec {
    DbLog::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;

    // EXTENDS BEYOND END OF STRUCT
    char name[1]; // has terminating null
};
struct MetricUpdateRec {
    DbLog::Record hdr;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
};
struct MetricUpdateSamplesRec {
    DbLog::Record hdr;
    uint16_t refPos;
    uint32_t refPage;
};

//---------------------------------------------------------------------------
// Sample
struct SampleInitRec {
    DbLog::Record hdr;
    uint32_t id;
    DbSampleType sampleType;
    TimePoint pageTime;
    uint16_t lastSample;
};
struct SampleUpdateRec {
    DbLog::Record hdr;
    uint16_t firstSample;
    uint16_t lastSample;
    double value;
};
struct SampleUpdateTimeRec {
    DbLog::Record hdr;
    TimePoint pageTime;
};

// Update (with or without last) is also an implicit transaction
struct SampleUpdateFloat64TxnRec {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t pos;
    double value;
};
struct SampleUpdateFloat32TxnRec {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t pos;
    float value;
};
struct SampleUpdateInt32TxnRec {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t pos;
    int32_t value;
};
struct SampleUpdateInt16TxnRec {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t pos;
    int16_t value;
};
struct SampleUpdateInt8TxnRec {
    DbLogRecType type;
    uint32_t pgno;
    uint16_t pos;
    int8_t value;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
// static
uint16_t DbLog::size(const Record * log) {
    switch (log->type) {
    case kRecTypeCommitCheckpoint:
        return sizeof(CheckpointCommitRec);
    case kRecTypeTxnBegin:
    case kRecTypeTxnCommit:
        return sizeof(TransactionRec);
    case kRecTypeZeroInit:
    case kRecTypePageFree:
        return sizeof(Record);
    case kRecTypeSegmentAlloc:
    case kRecTypeSegmentFree:
        return sizeof(SegmentUpdateRec);
    case kRecTypeRadixInit:
        return sizeof(RadixInitRec);
    case kRecTypeRadixInitList: {
        auto rec = reinterpret_cast<const RadixInitListRec *>(log);
        return offsetof(RadixInitListRec, pages)
            + rec->numPages * sizeof(*rec->pages);
    }
    case kRecTypeRadixErase:
        return sizeof(RadixEraseRec);
    case kRecTypeRadixPromote:
        return sizeof(RadixPromoteRec);
    case kRecTypeRadixUpdate:
        return sizeof(RadixUpdateRec);
    case kRecTypeMetricInit: {
        auto rec = reinterpret_cast<const MetricInitRec *>(log);
        return offsetof(MetricInitRec, name)
            + (uint16_t) strlen(rec->name) + 1;
    }
    case kRecTypeMetricUpdate:
        return sizeof(MetricUpdateRec);
    case kRecTypeMetricClearSamples:
        return sizeof(Record);
    case kRecTypeMetricUpdateLast:
    case kRecTypeMetricUpdateLastAndIndex:
        return sizeof(MetricUpdateSamplesRec);
    case kRecTypeSampleInit:
        return sizeof(SampleInitRec);
    case kRecTypeSampleUpdateFloat32Txn:
    case kRecTypeSampleUpdateFloat32LastTxn:
        return sizeof(SampleUpdateFloat32TxnRec);
    case kRecTypeSampleUpdateFloat64Txn:
    case kRecTypeSampleUpdateFloat64LastTxn:
        return sizeof(SampleUpdateFloat64TxnRec);
    case kRecTypeSampleUpdateInt8Txn:
    case kRecTypeSampleUpdateInt8LastTxn:
        return sizeof(SampleUpdateInt8TxnRec);
    case kRecTypeSampleUpdateInt16Txn:
    case kRecTypeSampleUpdateInt16LastTxn:
        return sizeof(SampleUpdateInt16TxnRec);
    case kRecTypeSampleUpdateInt32Txn:
    case kRecTypeSampleUpdateInt32LastTxn:
        return sizeof(SampleUpdateInt32TxnRec);
    case kRecTypeSampleUpdate:
    case kRecTypeSampleUpdateLast:
        return sizeof(SampleUpdateRec);
    case kRecTypeSampleUpdateTime:
        return sizeof(SampleUpdateTimeRec);
    case kRecType_NextAvailable:
        // only defined to suppress "not handled" warning
        break;
    }

    logMsgCrash() << "Unknown log record type, " << log->type;
    return 0;
}

//===========================================================================
// static
uint32_t DbLog::getPgno(const Record * log) {
    assert(log->type > kRecTypeTxnCommit);
    switch (log->type) {
    case kRecTypeSampleUpdateFloat32Txn:
    case kRecTypeSampleUpdateFloat64Txn:
    case kRecTypeSampleUpdateInt8Txn:
    case kRecTypeSampleUpdateInt16Txn:
    case kRecTypeSampleUpdateInt32Txn:
    case kRecTypeSampleUpdateFloat32LastTxn:
    case kRecTypeSampleUpdateFloat64LastTxn:
    case kRecTypeSampleUpdateInt8LastTxn:
    case kRecTypeSampleUpdateInt16LastTxn:
    case kRecTypeSampleUpdateInt32LastTxn:
        return reinterpret_cast<const SampleUpdateInt8TxnRec *>(log)->pgno;
    default:
        return log->pgno;
    }
}

//===========================================================================
// static
uint16_t DbLog::getLocalTxn(const DbLog::Record * log) {
    assert(log->type >= kRecTypeTxnBegin);
    switch (log->type) {
    case kRecTypeTxnBegin:
    case kRecTypeTxnCommit:
        return reinterpret_cast<const TransactionRec *>(log)->localTxn;
    case kRecTypeSampleUpdateFloat32Txn:
    case kRecTypeSampleUpdateFloat64Txn:
    case kRecTypeSampleUpdateInt8Txn:
    case kRecTypeSampleUpdateInt16Txn:
    case kRecTypeSampleUpdateInt32Txn:
    case kRecTypeSampleUpdateFloat32LastTxn:
    case kRecTypeSampleUpdateFloat64LastTxn:
    case kRecTypeSampleUpdateInt8LastTxn:
    case kRecTypeSampleUpdateInt16LastTxn:
    case kRecTypeSampleUpdateInt32LastTxn:
        return 0;
    default:
        return log->localTxn;
    }
}

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
    log((Record *) &rec, sizeof(rec));
}

//===========================================================================
uint64_t DbLog::logBeginTxn(uint16_t localTxn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnBegin;
    rec.localTxn = localTxn;
    auto lsn = log((Record *) &rec, sizeof(rec));
    return getTxn(lsn, localTxn);
}

//===========================================================================
void DbLog::logCommit(uint64_t txn) {
    TransactionRec rec;
    rec.type = kRecTypeTxnCommit;
    rec.localTxn = getLocalTxn(txn);
    log((Record *) &rec, sizeof(rec));
}

//===========================================================================
void DbLog::logAndApply(uint64_t txn, Record * rec, size_t bytes) {
    assert(bytes >= sizeof(DbLog::Record));
    if (txn)
        rec->localTxn = getLocalTxn(txn);
    auto lsn = log(rec, bytes);
    apply(lsn, rec, nullptr);
}

//===========================================================================
void DbLog::apply(uint64_t lsn, const Record * log, AnalyzeData * data) {
    switch (log->type) {
    case kRecTypeCommitCheckpoint:
        if (data) {
            auto rec = reinterpret_cast<const CheckpointCommitRec *>(log);
            applyCommitCheckpoint(*data, lsn, rec->startLsn);
        }
        break;
    case kRecTypeTxnBegin:
        if (data) {
            auto rec = reinterpret_cast<const TransactionRec *>(log);
            applyBeginTxn(*data, lsn, rec->localTxn);
        }
        break;
    case kRecTypeTxnCommit:
        if (data) {
            auto rec = reinterpret_cast<const TransactionRec *>(log);
            applyCommit(*data, lsn, rec->localTxn);
        }
        break;
    default:
        if (data) {
            applyRedo(*data, lsn, log);
        } else {
            applyUpdate(lsn, log);
        }
        break;
    }
}

//===========================================================================
void DbLog::applyUpdate(void * page, const Record * log) {
    switch (log->type) {
    default:
        logMsgCrash() << "unknown log record type, " << log->type;
        return;
    case kRecTypeZeroInit:
        return m_data.applyZeroInit(page);
    case kRecTypePageFree:
        return m_data.applyPageFree(page);
    case kRecTypeSegmentAlloc: {
        auto rec = reinterpret_cast<const SegmentUpdateRec *>(log);
        return m_data.applySegmentUpdate(page, rec->refPage, false);
    }
    case kRecTypeSegmentFree: {
        auto rec = reinterpret_cast<const SegmentUpdateRec *>(log);
        return m_data.applySegmentUpdate(page, rec->refPage, true);
    }
    case kRecTypeRadixInit: {
        auto rec = reinterpret_cast<const RadixInitRec *>(log);
        return m_data.applyRadixInit(
            page,
            rec->id,
            rec->height,
            nullptr,
            nullptr
        );
    }
    case kRecTypeRadixInitList: {
        auto rec = reinterpret_cast<const RadixInitListRec *>(log);
        return m_data.applyRadixInit(
            page,
            rec->id,
            rec->height,
            rec->pages,
            rec->pages + rec->numPages
        );
    }
    case kRecTypeRadixErase: {
        auto rec = reinterpret_cast<const RadixEraseRec *>(log);
        return m_data.applyRadixErase(page, rec->firstPos, rec->lastPos);
    }
    case kRecTypeRadixPromote: {
        auto rec = reinterpret_cast<const RadixPromoteRec *>(log);
        return m_data.applyRadixPromote(page, rec->refPage);
    }
    case kRecTypeRadixUpdate: {
        auto rec = reinterpret_cast<const RadixUpdateRec *>(log);
        return m_data.applyRadixUpdate(page, rec->refPos, rec->refPage);
    }

    case kRecTypeMetricInit: {
        auto rec = reinterpret_cast<const MetricInitRec *>(log);
        return m_data.applyMetricInit(
            page,
            rec->id,
            rec->name,
            rec->sampleType,
            rec->retention,
            rec->interval
        );
    }
    case kRecTypeMetricUpdate: {
        auto rec = reinterpret_cast<const MetricUpdateRec *>(log);
        return m_data.applyMetricUpdate(
            page,
            rec->sampleType,
            rec->retention,
            rec->interval
        );
    }
    case kRecTypeMetricClearSamples:
        return m_data.applyMetricClearSamples(page);
    case kRecTypeMetricUpdateLast: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            page,
            rec->refPos,
            rec->refPage,
            false
        );
    }
    case kRecTypeMetricUpdateLastAndIndex: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            page,
            rec->refPos,
            rec->refPage,
            true
        );
    }

    case kRecTypeSampleInit: {
        auto rec = reinterpret_cast<const SampleInitRec *>(log);
        return m_data.applySampleInit(
            page,
            rec->id,
            rec->sampleType,
            rec->pageTime,
            rec->lastSample
        );
    }
    case kRecTypeSampleUpdate: {
        auto rec = reinterpret_cast<const SampleUpdateRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->firstSample,
            rec->lastSample,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateLast: {
        auto rec = reinterpret_cast<const SampleUpdateRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->firstSample,
            rec->lastSample,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateTime: {
        auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(log);
        return m_data.applySampleUpdateTime(page, rec->pageTime);
    }

    case kRecTypeSampleUpdateFloat32Txn: {
        auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateFloat64Txn: {
        auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateInt8Txn: {
        auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateInt16Txn: {
        auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateInt32Txn: {
        auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateFloat32LastTxn: {
        auto rec = reinterpret_cast<const SampleUpdateFloat32TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateFloat64LastTxn: {
        auto rec = reinterpret_cast<const SampleUpdateFloat64TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateInt8LastTxn: {
        auto rec = reinterpret_cast<const SampleUpdateInt8TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateInt16LastTxn: {
        auto rec = reinterpret_cast<const SampleUpdateInt16TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateInt32LastTxn: {
        auto rec = reinterpret_cast<const SampleUpdateInt32TxnRec *>(log);
        return m_data.applySampleUpdate(
            page,
            rec->pos,
            rec->pos,
            rec->value,
            true
        );
    }

    } // end case
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
template<typename T>
pair<T *, size_t> DbTxn::alloc(
    DbLogRecType type,
    uint32_t pgno,
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

//===========================================================================
void DbTxn::log(DbLog::Record * rec, size_t bytes) {
    if (!m_txn)
        m_txn = m_log.beginTxn();
    m_log.logAndApply(m_txn, rec, bytes);
}

//===========================================================================
void DbTxn::logZeroInit(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeZeroInit, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logPageFree(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypePageFree, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logSegmentUpdate(
    uint32_t pgno,
    uint32_t refPage,
    bool free
) {
    auto [rec, bytes] = alloc<SegmentUpdateRec>(
        free ? kRecTypeSegmentFree : kRecTypeSegmentAlloc,
        pgno
    );
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixInit(
    uint32_t pgno,
    uint32_t id,
    uint16_t height,
    const uint32_t * firstPage,
    const uint32_t * lastPage
) {
    if (firstPage == lastPage) {
        auto [rec, bytes] = alloc<RadixInitRec>(kRecTypeRadixInit, pgno);
        rec->id = id;
        rec->height = height;
        log(&rec->hdr, bytes);
        return;
    }

    auto count = lastPage - firstPage;
    assert(count <= numeric_limits<uint16_t>::max());
    auto extra = count * sizeof(*firstPage);
    auto offset = offsetof(RadixInitListRec, pages);
    auto [rec, bytes] = alloc<RadixInitListRec>(
        kRecTypeRadixInitList,
        pgno,
        offset + extra
    );
    rec->id = id;
    rec->height = height;
    rec->numPages = (uint16_t) count;
    memcpy(rec->pages, firstPage, extra);
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixErase(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    auto [rec, bytes] = alloc<RadixEraseRec>(kRecTypeRadixErase, pgno);
    rec->firstPos = (uint16_t) firstPos;
    rec->lastPos = (uint16_t) lastPos;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixPromote(uint32_t pgno, uint32_t refPage) {
    auto [rec, bytes] = alloc<RadixPromoteRec>(kRecTypeRadixPromote, pgno);
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixUpdate(
    uint32_t pgno,
    size_t refPos,
    uint32_t refPage
) {
    auto [rec, bytes] = alloc<RadixUpdateRec>(kRecTypeRadixUpdate, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricInit(
    uint32_t pgno,
    uint32_t id,
    string_view name,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto extra = name.size() + 1;
    auto offset = offsetof(MetricInitRec, name);
    auto [rec, bytes] = alloc<MetricInitRec>(
        kRecTypeMetricInit,
        pgno,
        offset + extra
    );
    rec->id = id;
    rec->sampleType = sampleType;
    rec->retention = retention;
    rec->interval = interval;
    memcpy(rec->name, name.data(), extra - 1);
    rec->name[extra] = 0;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdate(
    uint32_t pgno,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    auto [rec, bytes] = alloc<MetricUpdateRec>(kRecTypeMetricUpdate, pgno);
    rec->sampleType = sampleType;
    rec->retention = retention;
    rec->interval = interval;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricClearSamples(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeMetricClearSamples, pgno);
    log(rec, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdateSamples(
    uint32_t pgno,
    size_t refPos,
    uint32_t refPage,
    bool updateIndex
) {
    auto type = updateIndex
        ? kRecTypeMetricUpdateLastAndIndex
        : kRecTypeMetricUpdateLast;
    auto [rec, bytes] = alloc<MetricUpdateSamplesRec>(type, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleInit(
    uint32_t pgno,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample
) {
    auto [rec, bytes] = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    rec->sampleType = sampleType;
    rec->pageTime = pageTime;
    rec->lastSample = (uint16_t) lastSample;
    log(&rec->hdr, bytes);
}

//===========================================================================
// This one is not like the others, it represents a transaction with just
// a single value update.
void DbTxn::logSampleUpdateTxn(
    uint32_t pgno,
    size_t pos,
    double value,
    bool updateLast
) {
    if (m_txn)
        return logSampleUpdate(pgno, pos, pos, value, updateLast);

    union {
        SampleUpdateFloat32TxnRec f32;
        SampleUpdateFloat64TxnRec f64;
        SampleUpdateInt8TxnRec i8;
        SampleUpdateInt16TxnRec i16;
        SampleUpdateInt32TxnRec i32;
    } tmp;
    assert(pos <= numeric_limits<decltype(tmp.i8.pos)>::max());
    size_t bytes{0};
    tmp.i8.pgno = pgno;
    tmp.i8.pos = (uint16_t) pos;
    if (auto ival = (int32_t) value; ival != value) {
        if (auto fval = (float) value; fval == value) {
            tmp.f32.type = updateLast
                ? kRecTypeSampleUpdateFloat32LastTxn
                : kRecTypeSampleUpdateFloat32Txn;
            tmp.f32.value = fval;
            bytes = sizeof(tmp.f32);
        } else {
            tmp.f64.type = updateLast
                ? kRecTypeSampleUpdateFloat64LastTxn
                : kRecTypeSampleUpdateFloat64Txn;
            tmp.f64.value = value;
            bytes = sizeof(tmp.f64);
        }
    } else {
        if ((int8_t) ival == ival) {
            tmp.i8.type = updateLast
                ? kRecTypeSampleUpdateInt8LastTxn
                : kRecTypeSampleUpdateInt8Txn;
            tmp.i8.value = (int8_t) ival;
            bytes = sizeof(tmp.i8);
        } else if ((int16_t) ival == ival) {
            tmp.i16.type = updateLast
                ? kRecTypeSampleUpdateInt16LastTxn
                : kRecTypeSampleUpdateInt16Txn;
            tmp.i16.value = (int16_t) ival;
            bytes = sizeof(tmp.i16);
        } else {
            tmp.i32.type = updateLast
                ? kRecTypeSampleUpdateInt32LastTxn
                : kRecTypeSampleUpdateInt32Txn;
            tmp.i32.value = ival;
            bytes = sizeof(tmp.i32);
        }
    }
    m_log.logAndApply(0, (DbLog::Record *) &tmp, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdate(
    uint32_t pgno,
    size_t firstSample,
    size_t lastSample,
    double value,
    bool updateLast
) {
    auto type = updateLast ? kRecTypeSampleUpdateLast : kRecTypeSampleUpdate;
    auto [rec, bytes] = alloc<SampleUpdateRec>(type, pgno);
    assert(firstSample <= lastSample);
    assert(lastSample <= numeric_limits<decltype(rec->firstSample)>::max());
    rec->firstSample = (uint16_t) firstSample;
    rec->lastSample = (uint16_t) lastSample;
    rec->value = value;
    log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdateTime(uint32_t pgno, TimePoint pageTime) {
    auto [rec, bytes] = alloc<SampleUpdateTimeRec>(kRecTypeSampleUpdateTime, pgno);
    rec->pageTime = pageTime;
    log(&rec->hdr, bytes);
}
