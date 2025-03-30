// Copyright Glen Knowles 2017 - 2025.
// Distributed under the Boost Software License, Version 1.0.
//
// dbmetricdata.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const DbSampleType kDefaultSampleType = kSampleTypeFloat32;
constexpr Duration kDefaultRetention = 7 * 24h;
constexpr Duration kDefaultInterval = 1min;
static_assert(kDefaultRetention >= kDefaultInterval);

const unsigned kMaxMetricNameLen = 255;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());


/****************************************************************************
*
*   Private
*
***/

#pragma pack(push, 1)

struct DbData::SamplePage {
    static const auto kPageType = DbPageType::kSample;
    DbPageHeader hdr;

    // Time of first sample on the page.
    TimePoint firstTime;

    // Time, value, position, and delta of the last sample on the page.
    TimePoint lastTime;
    uint16_t lastBitPos;

    DbSampleType sampleType;

    // First byte
    //  bit 2 - has new time delta
    //  bit 1 - has new value
    //  bit 0 - has repeat count
    // if repeat count
    //  2 byte - repeat count
    // if new time delta
    //  8 byte - duration
    // if new value
    //  4 byte - value

    // EXTENDS BEYOND END OF STRUCT
    uint64_t data[1];
};

#pragma pack(pop)


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCount = uperf("db.metrics (total)");

static auto & s_perfAncient = uperf("db.samples ignored (old)");
static auto & s_perfDup = uperf("db.samples ignored (dup)");
static auto & s_perfChange = uperf("db.samples changed");
static auto & s_perfAdd = uperf("db.samples added");


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
constexpr uint32_t pagesPerSegment(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return uint32_t(CHAR_BIT * pageSize / 2);
}

//===========================================================================
constexpr size_t segmentSize(size_t pageSize) {
    static_assert(CHAR_BIT == 8);
    return pageSize * pagesPerSegment(pageSize);
}

//===========================================================================
constexpr pair<pgno_t, size_t> segmentPage(pgno_t pgno, size_t pageSize) {
    auto pps = pagesPerSegment(pageSize);
    auto segPage = pgno / pps * pps;
    auto segPos = pgno % pps;
    return {(pgno_t) segPage, segPos};
}

//===========================================================================
constexpr size_t sampleTypeSize(DbSampleType type) {
    switch (type) {
    case kSampleTypeInvalid:
    case kSampleTypes:
        break;
    case kSampleTypeFloat32: return sizeof(float);
    case kSampleTypeFloat64: return sizeof(double);
    case kSampleTypeInt8: return sizeof(int8_t);
    case kSampleTypeInt16: return sizeof(int16_t);
    case kSampleTypeInt32: return sizeof(int32_t);
    }
    assert(!"invalid DbSampleType enum value");
    return 0;
}

//===========================================================================
static void noSamples(
    IDbDataNotify * notify,
    uint32_t id,
    string_view name,
    DbSampleType stype,
    TimePoint first,
    Duration interval
) {
    if (notify) {
        DbSeriesInfo info{};
        info.id = id;
        info.name = name;
        info.type = stype;
        info.first = first;
        info.last = first;
        info.interval = interval;
        if (notify->onDbSeriesStart(info))
            notify->onDbSeriesEnd(id);
    }
}


/****************************************************************************
*
*   SampleIndexRec
*
***/

namespace {

struct SampleIndexRec {
    TimePoint time;
    pgno_t pgno;
};

} // namespace

//===========================================================================
static string trieKey(const SampleIndexRec & rec) {
    auto time = rec.time.time_since_epoch().count();
    string out(sizeof time, 0);
    hton64(out.data(), time);
    out += DbData::trieKeyMin(rec.pgno);
    return out;
}

//===========================================================================
static bool parseTrieKey(SampleIndexRec * out, string_view val) {
    if (val.size() < sizeof(Duration::rep)) {
        *out = {};
        return false;
    }
    auto count = ntoh64(val.data());
    out->time = TimePoint(Duration(count));
    val.remove_prefix(sizeof(Duration::rep));
    underlying_type_t<pgno_t> pgno = 0;
    if (val.size() > sizeof pgno) {
        *out = {};
        return false;
    }
    for (auto&& ch : val) {
        pgno = 256 * pgno + (uint8_t) ch;
    }
    out->pgno = static_cast<pgno_t>(pgno);
    return true;
}


/****************************************************************************
*
*   DbData
*
***/

/****************************************************************************
*
*   Metric index
*
***/

//===========================================================================
void DbData::metricClearCounters() {
    s_perfCount -= m_numMetrics;
}

//===========================================================================
bool DbData::loadMetric(
    DbTxn & txn,
    IDbDataNotify * notify,
    const string & val
) {
    uint32_t id;
    DbMetricInfo info;
    if (!parseTrieKey(&id, &info, val))
        return false;
    if (notify) {
        DbSeriesInfo out;
        out.id = id;
        out.name = info.name;
        out.type = info.type;
        out.last = out.first + info.retention;
        out.interval = info.interval;
        if (!notify->onDbSeriesStart(out))
            return false;
    }
    if (appStopping())
        return false;

    s_perfCount += 1;
    m_numMetrics += 1;
    return true;
}

//===========================================================================
bool DbData::loadMetrics(DbTxn & txn, IDbDataNotify * notify) {
    auto root = txn.roots().info->root;
    return trieVisitWithPrefix(
        txn,
        root,
        {},
        [notify, this](DbTxn & txn, const string & key) {
            return loadMetric(txn, notify, key);
    });
}

//===========================================================================
// static
string DbData::trieKey(uint32_t id, const DbMetricInfo & info) {
    string key;
    auto len = sizeof id + info.name.size()
        + sizeof info.creation + sizeof info.lastInfoWrite
        + sizeof info.type
        + sizeof info.retention + sizeof info.interval;
    key.resize(len);
    auto ptr = reinterpret_cast<std::byte *>(key.data());
    hton32(&ptr, id);
    *ptr++ = static_cast<std::byte>(info.type);
    hton64(&ptr, info.creation.time_since_epoch().count());
    hton64(&ptr, info.lastInfoWrite.time_since_epoch().count());
    hton64(&ptr, info.retention.count());
    hton64(&ptr, info.interval.count());
    memcpy(ptr, info.name.data(), info.name.size());
    ptr += info.name.size();
    assert(ptr == reinterpret_cast<std::byte *>(key.data() + key.size()));
    return key;
}

//===========================================================================
// static
bool DbData::parseTrieKey(
    uint32_t * id,
    DbMetricInfo * out,
    std::string_view val
) {
    auto minLen = sizeof *id
        + sizeof out->creation + sizeof out->lastInfoWrite
        + sizeof out->type
        + sizeof out->retention + sizeof out->interval;
    if (val.size() < minLen) {
        *id = 0;
        *out = {};
        return false;
    }
    auto base = reinterpret_cast<const std::byte *>(val.data());
    auto ptr = base;
    *id = ntoh32(&ptr);
    if (!*id) {
        *out = {};
        return false;
    }
    out->type = static_cast<DbSampleType>(ntoh8(&ptr));
    out->creation = TimePoint(Duration(ntoh64(&ptr)));
    out->lastInfoWrite = TimePoint(Duration(ntoh64(&ptr)));
    out->retention = Duration(ntoh64(&ptr));
    out->interval = Duration(ntoh64(&ptr));
    out->name = val.substr(ptr - base);
    return true;
}

//===========================================================================
DbMetricInfo DbData::getMetricInfo(DbTxn & txn, uint32_t id) {
    DbMetricInfo out = {};
    trieVisitWithPrefix(
        txn,
        txn.roots().info->root,
        trieKey(id),
        [&out](DbTxn & txn, const string & val) {
            uint32_t id;
            parseTrieKey(&id, &out, val);
            return false;
    });
    return out;
}

//===========================================================================
void DbData::getMetricInfo(IDbDataNotify * notify, DbTxn & txn, uint32_t id) {
    auto mi = getMetricInfo(txn, id);
    if (!mi.type)
        return noSamples(notify, id, {}, kSampleTypeInvalid, {}, {});

    DbSeriesInfoEx info;
    info.id = id;
    info.name = move(mi.name);
    info.type = mi.type;
    info.last = info.first + mi.retention;
    info.interval = mi.interval;
    info.retention = mi.retention;
    info.creation = mi.creation;
    info.lastInfoWrite = mi.lastInfoWrite;
    if (notify->onDbSeriesStart(info))
        notify->onDbSeriesEnd(id);
}

//===========================================================================
void DbData::updateMetric(
    DbTxn & txn,
    uint32_t id,
    const DbMetricInfo & from
) {
    assert(from.name.empty());
    // TODO: validate interval, retention, and type

    auto mi = getMetricInfo(txn, id);
    if (!mi.type)
        return;
    DbMetricInfo info = {};
    info.retention = from.retention.count() ? from.retention : mi.retention;
    info.interval = from.interval.count() ? from.interval : mi.interval;
    info.type = from.type ? from.type : mi.type;
    info.creation = !empty(from.creation) ? from.creation : mi.creation;
    if (mi.retention == info.retention
        && mi.interval == info.interval
        && mi.type == info.type
        && mi.creation == info.creation
    ) {
        return;
    }

    shared_lock lk{m_mposMut};
    // Remove all existing samples
    radixErase(txn, m_sampleRoot, id, id + 1);
    // TODO: erase entries from m_metricRoots->sampleTimeRoot or wherever
    // sample pages by start time are stored.
}

//===========================================================================
void DbData::insertMetric(DbTxn & txn, uint32_t id, string_view name) {
    assert(!name.empty());
    if (name.size() >= kMaxMetricNameLen)
        name = name.substr(0, kMaxMetricNameLen - 1);

    auto now = timeNow();
    DbMetricInfo info = {
        .name = string(name),
        .creation = now,
        .lastInfoWrite = now,
        .type = kDefaultSampleType,
        .retention = kDefaultRetention,
        .interval = kDefaultInterval,
    };

    // update indexes
    vector<TrieAction> actions = {
        TrieAction::kInsert,    // metric info by id
        TrieAction::kInsert,    // metric name
    };
    vector<shared_ptr<DbRootVersion>> roots = {
        txn.roots().info,
        txn.roots().name,
    };
    vector<string> keys = {
        trieKey(id, info),
        trieKey(name, id),
    };
    trieApply(txn, actions, roots, keys);

    // update in memory references
    m_numMetrics += 1;
}

//===========================================================================
bool DbData::eraseMetric(string * name, DbTxn & txn, uint32_t id) {
    auto mi = getMetricInfo(txn, id);
    if (!mi.type)
        return false;

    *name = mi.name;

    // erase samples
    eraseSamples(txn, id);

    // update name index
    vector<TrieAction> actions = {
        TrieAction::kErase,
        TrieAction::kErase,
    };
    vector<shared_ptr<DbRootVersion>> roots = {
        txn.roots().info,
        txn.roots().name,
    };
    vector<string> keys = {
        trieKey(id, mi),
        trieKey(*name, id),
    };
    trieApply(txn, actions, roots, keys);

    return true;
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
bool DbData::findLastSamplePage(
    DbTxn & txn,
    pgno_t * spno,
    uint32_t id,
    DbSampleType type,
    TimePoint time,
    double value
) {
    scoped_lock lk{m_mndxMut};
    DbTxn::PinScope pins(txn);
    if (radixFind(txn, spno, m_sampleRoot, id))
        return true;
    if (type) {
        // No pages, create page and add sample to it.
        *spno = allocPgno(txn);
        radixInsert(txn, m_sampleRoot, id, *spno);
        txn.walSampleInit(*spno, id, type, time, value);
        s_perfAdd += 1;
    }
    return false;
}

//===========================================================================
bool DbData::findSamplePage(
    DbTxn & txn,
    pgno_t * sipno,
    pgno_t * spno,
    uint32_t id,
    TimePoint time
) {
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        if (!radixFind(txn, sipno, m_sampleIndexRoot, id)) {
            *spno = {};
            return false;
        }
    }

    SampleIndexRec rec = {
        .time = time,
        .pgno = {},
    };
    auto key = ::trieKey(rec);
    DbPageHeap heap(&txn, this, 0, *sipno);
    StrTrieBase trie(&heap);
    auto i = trie.findLessEqual(key);
    auto found = (i == trie.end()) ? trie.front() : *i;
    if (!::parseTrieKey(&rec, found)) {
        logMsgFatal() << "findSamplePage(" << id << ", " << time
            << "): invalid entry in sample index";
        *spno = {};
        return false;
    }
    *spno = rec.pgno;
    return true;
}

//===========================================================================
void DbData::eraseSampleIndex(DbTxn & txn, uint32_t id) {
    pgno_t iroot = {};
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        iroot = radixSwapValue(txn, m_sampleIndexRoot, id, {});
    }
    if (iroot)
        trieClear(txn, iroot);
}

//===========================================================================
void DbData::eraseSamples(DbTxn & txn, uint32_t id) {
    pgno_t iroot = {};
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        iroot = radixSwapValue(txn, m_sampleIndexRoot, id, {});
        if (!iroot) {
            radixErase(txn, m_sampleRoot, id, id + 1);
            return;
        }
        radixSwapValue(txn, m_sampleRoot, id, {});
    }

    trieVisitWithPrefix(txn, iroot, {}, [this](auto & txn, auto & key) {
        SampleIndexRec rec;
        if (!::parseTrieKey(&rec, key)) {
            assert("Bad sample index entry");
        } else {
            freePage(txn, rec.pgno);
        }
        return true;
    });
    trieClear(txn, iroot);
}

//===========================================================================
template<typename T>
static double getSample(const T * out) {
    if constexpr (is_same_v<T, pgno_t>) {
        if (*out <= kMaxPageNum)
            return NAN;
        return (double) *out - (kMaxPageNum + kMaxPageNum / 2);
    } else if constexpr (is_floating_point_v<T>) {
        return *out;
    } else if constexpr (is_integral_v<T>) {
        constexpr auto maxval = numeric_limits<T>::max();
        constexpr auto minval = -maxval;
        T ival = *out;
        if (ival == minval - 1)
            return NAN;
        return ival;
    } else {
        assert(!"Sample type must be numeric");
        return NAN;
    }
}

//===========================================================================
void DbData::updateSample(
    DbTxn & txn,
    uint32_t id,
    TimePoint time,
    double value
) {
    assert(!empty(time));

    // Ensure all info about the last page is loaded, the expectation is that
    // almost all updates are to the last page.
    auto mi = getMetricInfo(txn, id);
    if (!mi.type)
        return;

    // Round time down to metric's sampling interval.
    time -= time.time_since_epoch() % mi.interval;

    //-----------------------------------------------------------------------
    // Find page that should contain sample
    pgno_t spno;
    pgno_t sipno = npos;
    if (!findLastSamplePage(txn, &spno, id, mi.type, time, value)) {
        // No existing samples, new page was created with this sample.
        return;
    }
    auto sp = txn.pin<SamplePage>(spno);
    if (time >= sp->firstTime) {
        // Updating sample on last page.
        assert(spno);
    } else {
        // Sample older than last page.
        auto firstSampleTime = sp->lastTime - mi.retention;
        if (time < firstSampleTime) {
            // Sample older than retention, ignore it.
            s_perfAncient += 1;
            return;
        }
        // Search sample index for containing page.
        if (pgno_t pgno; !findSamplePage(txn, &sipno, &pgno, id, time)) {
            // No sample index, add to last page.
            assert(spno);
        } else {
            // Update sample on page found in index.
            spno = pgno;
            sp = txn.pin<SamplePage>(spno);
        }
    }

    //-----------------------------------------------------------------------
    // Further in the future than the retention period?
    if (time > sp->lastTime + mi.retention) {
        // Remove all samples and add as new initial sample.
        eraseSamples(txn, id);
        // Add new sample in green field.
        updateSample(txn, id, time, value);
        return;
    }

    //-----------------------------------------------------------------------
    // Update page
#if 1
    return;
#else
    if (time > sp->lastTime) {
        // Sample belongs at end of page.
        if (update delta equals last && repeat counter < max reps) {
            // Increment repeat counter.
            return;
        }
        if (not room for new entry) {
            // Remove ancient entries.
        }
        if (has room for new entry) {
            // Append new sample.
            return;
        }
        // Add new page with just the new sample.
        return;
    }
    // Find where sample belongs on the page.
    if (update exactly equals found) {
        s_perfDup += 1;
        return;
    }
    if (update delta equals found && repeat counter < max reps) {
        // Increment repeat counter.
        return;
    }
    if (between entries) {
        if (not room for new entry) {
            if (no sample index) {
                // Create sample index.
                // Add current page to sample index.
            }
            if (last page) {
                // splitPos = 90%
            } else {
                // splitPos = 50%
            }
            // Create new sample starting with splitPos.
            // Add new splitPos page to sample index.
            // Truncate current page to splitPos.
        }
        updateSample(txn, id, time, value);
        return;
    }
    unreachable();
#endif
}

//===========================================================================
void DbData::onWalApplySampleInit(
    void * ptr,
    uint32_t id,
    DbSampleType type,
    TimePoint time,
    double value
) {
    auto sp = static_cast<SamplePage *>(ptr);
    if (sp->hdr.type == DbPageType::kFree) {
        memset((char *) sp + sizeof(sp->hdr), 0, m_pageSize - sizeof(sp->hdr));
    } else {
        assert(sp->hdr.type == DbPageType::kInvalid);
    }
    sp->hdr.type = sp->kPageType;
    sp->hdr.id = id;
    sp->firstTime = time;
    sp->lastTime = time;
    sp->sampleType = type;

    // TODO: write value to sp->data[]
}

//===========================================================================
void DbData::onWalApplySampleUpdate(
    void * ptr,
    size_t firstPos,
    size_t lastPos,
    double value,
    bool updateLast
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
}

//===========================================================================
void DbData::onWalApplySampleUpdateTime(
    void * ptr,
    TimePoint firstTime,
    TimePoint lastTime
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
    if (firstTime == TimePoint{})
        sp->firstTime = firstTime;
    if (lastTime == TimePoint{})
        sp->lastTime = lastTime;
}

//===========================================================================
void DbData::getSamples(
    IDbDataNotify * notify,
    DbTxn & txn,
    uint32_t id,
    TimePoint first,
    TimePoint last,
    unsigned presamples
) {
    auto mi = getMetricInfo(txn, id);
    if (!mi.type)
        return noSamples(notify, id, {}, kSampleTypeInvalid, {}, {});

    // round time to metric's sampling interval
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    // Expand range to include presamples.
    first -= presamples * mi.interval;

    pgno_t lastPage = {};
    if (!findLastSamplePage(txn, &lastPage, id))
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);

    auto sp = txn.pin<SamplePage>(lastPage);
    if (first > last || first > sp->lastTime)
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);

    vector<pgno_t> pgnos;
    if (first >= sp->firstTime) {
        pgnos.push_back(lastPage);
    } else {
        // Get list from the metric's sample index.
    }
    if (pgnos.empty())
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);

    DbSeriesInfo dsi;
    dsi.id = id;
    dsi.name = mi.name;
    dsi.type = mi.type;
    dsi.interval = mi.interval;
    unsigned count = 0;
    for (auto && spno : pgnos) {
        // Loop through entries on page
        sp = txn.pin<SamplePage>(spno);
        for (auto i = 0; i < 2; ++i) {
            double value = NAN;
            if (sp) {
                value = 0;
                if (isnan(value))
                    continue;
            }
            if (!count++) {
                dsi.first = first;
                dsi.last = last + mi.interval;
                if (!notify->onDbSeriesStart(dsi))
                    return;
            }
            if (!notify->onDbSample(id, first, value))
                return;
        }
    }
    if (!count) {
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);
    } else {
        notify->onDbSeriesEnd(id);
    }
}
