// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dblogrec.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

enum DbLogRecType : uint8_t {
    kRecTypeZeroInit,           // [master]
    kRecTypePageFree,           // [any]
    kRecTypeSegmentInit,        // [segment]
    kRecTypeSegmentAlloc,       // [master/segment] refPage
    kRecTypeSegmentFree,        // [master/segment] refPage
    kRecTypeRadixInit,          // [radix] id, height
    kRecTypeRadixInitList,      // [radix] id, height, page list
    kRecTypeRadixErase,         // [metric/radix] firstPos, lastPos
    kRecTypeRadixPromote,       // [radix] refPage
    kRecTypeRadixUpdate,        // [radix] refPos, refPage
    kRecTypeMetricInit,         // [metric] name, id, retention, interval
    kRecTypeMetricUpdate,       // [metric] retention, interval
    kRecTypeMetricClearSamples, // [metric] (clears index & last)
    kRecTypeMetricUpdateLast,   // [metric] refPos, refPage
    kRecTypeMetricUpdateLastAndIndex, // [metric] refPos, refPage
    kRecTypeSampleInit,         // [sample] id, pageTime, lastPos
    kRecTypeSampleUpdate,       // [sample] first, last, value
                                //   [first, last) = NANs, last = value
    kRecTypeSampleUpdateLast,   // [sample] first, last, value
                                //   [first, last) = NANs, last = value
    kRecTypeSampleUpdateTime,   // [sample] pageTime (pos=0, samples[0]=NAN)

    kRecTypeTxnBegin,           // N/A
    kRecTypeTxnCommit,          // N/A
};

#pragma pack(push)
#pragma pack(1)

struct DbLog::Record {
    DbLogRecType type;
    uint32_t pgno;
    union {
        uint64_t full;
        struct {
            uint64_t seq : 48;
            uint64_t txn : 16;
        } u;
    } lsn;
};

namespace {

//---------------------------------------------------------------------------
// Transaction
struct TransactionRec {
    DbLogRecType type;
    uint16_t txn;
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
    Duration retention;
    Duration interval;

    // EXTENDS BEYOND END OF STRUCT
    char name[1]; // has terminating null
};
struct MetricUpdateRec {
    DbLog::Record hdr;
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
    TimePoint pageTime;
    uint16_t lastSample;
};
struct SampleUpdateRec {
    DbLog::Record hdr;
    uint16_t firstSample;
    uint16_t lastSample;
    float value;
};
struct SampleUpdateTimeRec {
    DbLog::Record hdr;
    TimePoint pageTime;
};

} // namespace

#pragma pack(pop)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
inline static size_t size(const DbLog::Record * log) {
    switch (log->type) {
    case kRecTypeZeroInit:
    case kRecTypePageFree:
    case kRecTypeSegmentInit:
        return sizeof(DbLog::Record);
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
            + strlen(rec->name) + 1;
    }
    case kRecTypeMetricUpdate:
        return sizeof(MetricUpdateRec);
    case kRecTypeMetricClearSamples:
        return sizeof(DbLog::Record);
    case kRecTypeMetricUpdateLast:
    case kRecTypeMetricUpdateLastAndIndex:
        return sizeof(MetricUpdateSamplesRec);
    case kRecTypeSampleInit:
        return sizeof(SampleInitRec);
    case kRecTypeSampleUpdate:
    case kRecTypeSampleUpdateLast:
        return sizeof(SampleUpdateRec);
    case kRecTypeSampleUpdateTime:
        return sizeof(SampleUpdateTimeRec);
    case kRecTypeTxnBegin:
    case kRecTypeTxnCommit:
        return sizeof(TransactionRec);
    }

    logMsgCrash() << "Unknown log record type, " << log->type;
    return 0;
}


/****************************************************************************
*
*   DbLog
*
***/

//===========================================================================
// static
uint32_t DbLog::getPgno(const DbLog::Record * log) {
    return log->pgno;
}

//===========================================================================
// static
uint64_t DbLog::getLsn(const DbLog::Record * log) {
    return log->lsn.full;
}

//===========================================================================
void DbLog::logBeginTxn(unsigned txn) {
    auto bytes = sizeof(TransactionRec);
    auto rec = (TransactionRec *) alloc(bytes);
    rec->type = kRecTypeTxnBegin;
    rec->txn = (uint16_t) txn;
    log((Record *) rec, bytes);
}

//===========================================================================
void DbLog::logCommit(unsigned txn) {
    auto bytes = sizeof(TransactionRec);
    auto rec = (TransactionRec *) alloc(bytes);
    rec->type = kRecTypeTxnCommit;
    rec->txn = m_lastTxn;
    log((Record *) rec, bytes);
}

//===========================================================================
void * DbLog::alloc(size_t bytes) {
    static string s_rec;
    s_rec.resize(bytes);
    return s_rec.data();
}

//===========================================================================
DbLog::Record * DbLog::alloc(
    unsigned txn,
    DbLogRecType type,
    uint32_t pgno,
    size_t bytes
) {
    assert(txn);
    auto lsn = ++m_lastLsn;
    assert(bytes >= sizeof(DbLog::Record));
    auto rec = (Record *) alloc(bytes);
    rec->lsn.u.txn = txn;
    rec->lsn.u.seq = lsn;
    rec->type = type;
    rec->pgno = pgno;
    return rec;
}

//===========================================================================
void DbLog::log(const Record * log, size_t bytes) {
    assert(bytes == size(log));

    switch (log->type) {
    case kRecTypeTxnBegin: {
        auto rec = reinterpret_cast<const TransactionRec *>(log);
        return applyBeginTxn(rec->txn);
    }
    case kRecTypeTxnCommit: {
        auto rec = reinterpret_cast<const TransactionRec *>(log);
        return applyCommit(rec->txn);
    }
    default:
        return apply(log);
    }
}

//===========================================================================
void DbLog::apply(void * hdr, const Record * log) {
    switch (log->type) {
    default:
        logMsgCrash() << "unknown log record type, " << log->type;
        return;
    case kRecTypeZeroInit:
        return m_data.applyZeroInit(hdr);
    case kRecTypePageFree:
        return m_data.applyPageFree(hdr);
    case kRecTypeSegmentInit:
        return m_data.applySegmentInit(hdr);
    case kRecTypeSegmentAlloc: {
        auto rec = reinterpret_cast<const SegmentUpdateRec *>(log);
        return m_data.applySegmentUpdate(hdr, rec->refPage, false);
    }
    case kRecTypeSegmentFree: {
        auto rec = reinterpret_cast<const SegmentUpdateRec *>(log);
        return m_data.applySegmentUpdate(hdr, rec->refPage, true);
    }
    case kRecTypeRadixInit: {
        auto rec = reinterpret_cast<const RadixInitRec *>(log);
        return m_data.applyRadixInit(
            hdr,
            rec->id,
            rec->height,
            nullptr,
            nullptr
        );
    }
    case kRecTypeRadixInitList: {
        auto rec = reinterpret_cast<const RadixInitListRec *>(log);
        return m_data.applyRadixInit(
            hdr,
            rec->id,
            rec->height,
            rec->pages,
            rec->pages + rec->numPages
        );
    }
    case kRecTypeRadixErase: {
        auto rec = reinterpret_cast<const RadixEraseRec *>(log);
        return m_data.applyRadixErase(hdr, rec->firstPos, rec->lastPos);
    }
    case kRecTypeRadixPromote: {
        auto rec = reinterpret_cast<const RadixPromoteRec *>(log);
        return m_data.applyRadixPromote(hdr, rec->refPage);
    }
    case kRecTypeRadixUpdate: {
        auto rec = reinterpret_cast<const RadixUpdateRec *>(log);
        return m_data.applyRadixUpdate(hdr, rec->refPos, rec->refPage);
    }

    case kRecTypeMetricInit: {
        auto rec = reinterpret_cast<const MetricInitRec *>(log);
        return m_data.applyMetricInit(
            hdr,
            rec->id,
            rec->name,
            rec->retention,
            rec->interval
        );
    }
    case kRecTypeMetricUpdate: {
        auto rec = reinterpret_cast<const MetricUpdateRec *>(log);
        return m_data.applyMetricUpdate(
            hdr,
            rec->retention,
            rec->interval
        );
    }
    case kRecTypeMetricClearSamples:
        return m_data.applyMetricClearSamples(hdr);
    case kRecTypeMetricUpdateLast: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            hdr,
            rec->refPos,
            rec->refPage,
            false
        );
    }
    case kRecTypeMetricUpdateLastAndIndex: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            hdr,
            rec->refPos,
            rec->refPage,
            true
        );
    }

    case kRecTypeSampleInit: {
        auto rec = reinterpret_cast<const SampleInitRec *>(log);
        return m_data.applySampleInit(
            hdr,
            rec->id,
            rec->pageTime,
            rec->lastSample
        );
    }
    case kRecTypeSampleUpdate: {
        auto rec = reinterpret_cast<const SampleUpdateRec *>(log);
        return m_data.applySampleUpdate(
            hdr,
            rec->firstSample,
            rec->lastSample,
            rec->value,
            false
        );
    }
    case kRecTypeSampleUpdateLast: {
        auto rec = reinterpret_cast<const SampleUpdateRec *>(log);
        return m_data.applySampleUpdate(
            hdr,
            rec->firstSample,
            rec->lastSample,
            rec->value,
            true
        );
    }
    case kRecTypeSampleUpdateTime: {
        auto rec = reinterpret_cast<const SampleUpdateTimeRec *>(log);
        return m_data.applySampleUpdateTime(hdr, rec->pageTime);
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
    return {(T *) m_log.alloc(m_txn, type, pgno, bytes), bytes};
}

//===========================================================================
void DbTxn::logZeroInit(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeZeroInit, pgno);
    m_log.log(rec, bytes);
}

//===========================================================================
void DbTxn::logPageFree(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypePageFree, pgno);
    m_log.log(rec, bytes);
}

//===========================================================================
void DbTxn::logSegmentInit(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeSegmentInit, pgno);
    m_log.log(rec, bytes);
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
    m_log.log(&rec->hdr, bytes);
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
        m_log.log(&rec->hdr, bytes);
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
    m_log.log(&rec->hdr, bytes);
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
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logRadixPromote(uint32_t pgno, uint32_t refPage) {
    auto [rec, bytes] = alloc<RadixPromoteRec>(kRecTypeRadixPromote, pgno);
    rec->refPage = refPage;
    m_log.log(&rec->hdr, bytes);
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
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricInit(
    uint32_t pgno,
    uint32_t id,
    string_view name,
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
    rec->retention = retention;
    rec->interval = interval;
    memcpy(rec->name, name.data(), extra);
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricUpdate(
    uint32_t pgno,
    Duration retention,
    Duration interval
) {
    auto [rec, bytes] = alloc<MetricUpdateRec>(kRecTypeMetricUpdate, pgno);
    rec->retention = retention;
    rec->interval = interval;
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logMetricClearSamples(uint32_t pgno) {
    auto [rec, bytes] = alloc<DbLog::Record>(kRecTypeMetricClearSamples, pgno);
    m_log.log(rec, bytes);
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
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleInit(
    uint32_t pgno,
    uint32_t id,
    TimePoint pageTime,
    size_t lastSample
) {
    auto [rec, bytes] = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    rec->pageTime = pageTime;
    rec->lastSample = (uint16_t) lastSample;
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdate(
    uint32_t pgno,
    size_t firstSample,
    size_t lastSample,
    float value,
    bool updateLast
) {
    auto type = updateLast ? kRecTypeSampleUpdateLast : kRecTypeSampleUpdate;
    auto [rec, bytes] = alloc<SampleUpdateRec>(type, pgno);
    assert(firstSample <= lastSample);
    assert(lastSample <= numeric_limits<decltype(rec->firstSample)>::max());
    rec->firstSample = (uint16_t) firstSample;
    rec->lastSample = (uint16_t) lastSample;
    rec->value = value;
    m_log.log(&rec->hdr, bytes);
}

//===========================================================================
void DbTxn::logSampleUpdateTime(uint32_t pgno, TimePoint pageTime) {
    auto [rec, bytes] = alloc<SampleUpdateTimeRec>(kRecTypeSampleUpdateTime, pgno);
    rec->pageTime = pageTime;
    m_log.log(&rec->hdr, bytes);
}
