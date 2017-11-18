// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dblog.cpp - tismet db
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
*   DbLog
*
***/

//===========================================================================
DbLog::DbLog(DbData & data, DbWork & work)
    : m_data(data)
    , m_work(work)
{}

//===========================================================================
uint64_t DbLog::beginTrans() {
    for (;;) {
        if (++m_lastTxn)
            break;
    }
    return m_lastTxn;
}

//===========================================================================
void DbLog::commit(uint64_t txn) {
}

//===========================================================================
DbLog::Record * DbLog::alloc(
    uint64_t txn,
    DbLogRecType type,
    uint32_t pgno,
    size_t bytes
) {
    static string s_rec;
    assert(txn);
    auto lsn = ++m_lastLsn;
    assert(bytes >= sizeof(DbLog::Record));
    s_rec.resize(bytes);
    auto rec = (Record *) s_rec.data();
    rec->lsn.u.txn = txn;
    rec->lsn.u.seq = lsn;
    rec->type = type;
    rec->pgno = pgno;
    return rec;
}

//===========================================================================
void DbLog::apply(const Record * log) {
    auto vptr = make_unique<char[]>(m_work.pageSize());
    auto ptr = new(vptr.get()) DbPageHeader;
    auto rptr = m_work.rptr(log->lsn.full, log->pgno);
    memcpy(ptr, rptr, m_work.pageSize());
    ptr->pgno = log->pgno;
    ptr->lsn = log->lsn.full;
    apply(ptr, log);
    m_work.writePage(ptr);
}

//===========================================================================
void DbLog::apply(void * hdr, const Record * log) {
    switch (log->type) {
    case kRecTypeMasterInit:
        return m_data.applyMasterInit(hdr);
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
    case kRecTypeMetricUpdateIndex: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            hdr,
            rec->refPos,
            rec->refPage,
            false,
            true
        );
    }
    case kRecTypeMetricUpdateLast: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            hdr,
            rec->refPos,
            rec->refPage,
            true,
            false
        );
    }
    case kRecTypeMetricUpdateIndexAndLast: {
        auto rec = reinterpret_cast<const MetricUpdateSamplesRec *>(log);
        return m_data.applyMetricUpdateSamples(
            hdr,
            rec->refPos,
            rec->refPage,
            true,
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

    case kRecTypeTxnCommit:
        abort();
    }
}


/****************************************************************************
*
*   DbTxn
*
***/

//===========================================================================
DbTxn::DbTxn(DbLog & log, DbWork & work)
    : m_log{log}
    , m_work{work}
{
    m_txn = m_log.beginTrans();
}

//===========================================================================
DbTxn::~DbTxn() {
    m_log.commit(m_txn);
}

//===========================================================================
template<typename T>
T * DbTxn::alloc(
    DbLogRecType type,
    uint32_t pgno,
    size_t bytes
) {
    assert(bytes >= sizeof(T));
    if (!m_txn)
        m_txn = m_log.beginTrans();
    return (T *) m_log.alloc(m_txn, type, pgno, bytes);
}

//===========================================================================
void DbTxn::logMasterInit(uint32_t pgno) {
    auto rec = alloc<DbLog::Record>(kRecTypeMasterInit, pgno);
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logPageFree(uint32_t pgno) {
    auto rec = alloc<DbLog::Record>(kRecTypePageFree, pgno);
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logSegmentInit(uint32_t pgno) {
    auto rec = alloc<DbLog::Record>(kRecTypeSegmentInit, pgno);
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logSegmentUpdate(
    uint32_t pgno,
    uint32_t refPage,
    bool free
) {
    auto rec = alloc<SegmentUpdateRec>(
        free ? kRecTypeSegmentFree : kRecTypeSegmentAlloc,
        pgno
    );
    rec->refPage = refPage;
    m_log.apply(&rec->hdr);
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
        auto rec = alloc<RadixInitRec>(kRecTypeRadixInit, pgno);
        rec->id = id;
        rec->height = height;
        m_log.apply(&rec->hdr);
        return;
    }

    auto count = lastPage - firstPage;
    assert(count <= numeric_limits<uint16_t>::max());
    auto bytes = count * sizeof(*firstPage);
    auto offset = offsetof(RadixInitListRec, pages);
    auto rec = alloc<RadixInitListRec>(
        kRecTypeRadixInitList,
        pgno,
        offset + bytes
    );
    rec->id = id;
    rec->height = height;
    rec->numPages = (uint16_t) count;
    memcpy(rec->pages, firstPage, bytes);
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logRadixErase(
    uint32_t pgno,
    size_t firstPos,
    size_t lastPos
) {
    auto rec = alloc<RadixEraseRec>(kRecTypeRadixErase, pgno);
    rec->firstPos = (uint16_t) firstPos;
    rec->lastPos = (uint16_t) lastPos;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logRadixPromote(uint32_t pgno, uint32_t refPage) {
    auto rec = alloc<RadixPromoteRec>(kRecTypeRadixPromote, pgno);
    rec->refPage = refPage;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logRadixUpdate(
    uint32_t pgno,
    size_t refPos,
    uint32_t refPage
) {
    auto rec = alloc<RadixUpdateRec>(kRecTypeRadixUpdate, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logMetricInit(
    uint32_t pgno,
    uint32_t id,
    string_view name,
    Duration retention,
    Duration interval
) {
    auto offset = offsetof(MetricInitRec, name);
    auto bytes = name.size() + 1;
    auto rec = alloc<MetricInitRec>(kRecTypeMetricInit, pgno, offset + bytes);
    rec->id = id;
    rec->retention = retention;
    rec->interval = interval;
    memcpy(rec->name, name.data(), bytes);
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logMetricUpdate(
    uint32_t pgno,
    Duration retention,
    Duration interval
) {
    auto rec = alloc<MetricUpdateRec>(kRecTypeMetricUpdate, pgno);
    rec->retention = retention;
    rec->interval = interval;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logMetricClearSamples(uint32_t pgno) {
    auto rec = alloc<DbLog::Record>(kRecTypeMetricClearSamples, pgno);
    m_log.apply(rec);
}

//===========================================================================
void DbTxn::logMetricUpdateSamples(
    uint32_t pgno,
    size_t refPos,
    uint32_t refPage,
    bool updateLast,
    bool updateIndex
) {
    assert(updateLast || updateIndex);
    static const DbLogRecType types[] = {
        DbLogRecType{0},
        kRecTypeMetricUpdateIndex,
        kRecTypeMetricUpdateLast,
        kRecTypeMetricUpdateIndexAndLast,
    };
    auto type = types[2 * updateLast + updateIndex];
    auto rec = alloc<MetricUpdateSamplesRec>(type, pgno);
    rec->refPos = (uint16_t) refPos;
    rec->refPage = refPage;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logSampleInit(
    uint32_t pgno,
    uint32_t id,
    TimePoint pageTime,
    size_t lastSample
) {
    auto rec = alloc<SampleInitRec>(kRecTypeSampleInit, pgno);
    rec->id = id;
    rec->pageTime = pageTime;
    rec->lastSample = (uint16_t) lastSample;
    m_log.apply(&rec->hdr);
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
    auto rec = alloc<SampleUpdateRec>(type, pgno);
    assert(firstSample <= lastSample);
    assert(lastSample <= numeric_limits<decltype(rec->firstSample)>::max());
    rec->firstSample = (uint16_t) firstSample;
    rec->lastSample = (uint16_t) lastSample;
    rec->value = value;
    m_log.apply(&rec->hdr);
}

//===========================================================================
void DbTxn::logSampleUpdateTime(uint32_t pgno, TimePoint pageTime) {
    auto rec = alloc<SampleInitRec>(kRecTypeSampleUpdateTime, pgno);
    rec->pageTime = pageTime;
    m_log.apply(&rec->hdr);
}
