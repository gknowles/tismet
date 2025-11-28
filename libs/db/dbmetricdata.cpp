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

    // Time of first and last sample on the page.
    TimePoint firstTime;
    TimePoint lastTime;

    pgno_t sampleIndex;

    // Bits used by sample data.
    uint16_t dataBits;

    // Type of data stored per sample (float32, float64, int8, ...).
    DbSampleType sampleType;

    // Padding to make the overall structure a multiple of uint64_t in size.
    // This allows the use of BitView/BitSpan on the data member.
    uint8_t pad[1];

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
// The data[] must be uint64_t align so that BitView/BitSpan can be used.
static_assert(sizeof(DbData::SamplePage) % alignof(uint64_t) == 0);

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
constexpr size_t sampleDataPerPage(DbSampleType type, size_t pageSize) {
    assert(pageSize > sizeof DbData::SamplePage);
    return pageSize - offsetof(DbData::SamplePage, data);
}

//===========================================================================
constexpr BitSpan sampleDataSpan(DbData::SamplePage * sp, size_t pageSize) {
    assert(pageSize > sizeof DbData::SamplePage);
    assert(offsetof(DbData::SamplePage, data) % alignof(uint64_t) == 0);
    auto base = sp->data;
    auto bytes = sampleDataPerPage(sp->sampleType, pageSize);
    assert(bytes % alignof(uint64_t) == 0);
    return { base, bytes / sizeof *base };
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
*   DbData - Metric index
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
        { TrieAction::kInsert, txn.roots().info,     trieKey(id, info) },
        { TrieAction::kInsert, txn.roots().idByName, trieKey(name, id) },
    };
    trieApply(txn, actions);

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

    // update indexes
    vector<TrieAction> actions = {
        { TrieAction::kErase, txn.roots().info,     trieKey(id, mi) },
        { TrieAction::kErase, txn.roots().idByName, trieKey(*name, id) },
    };
    trieApply(txn, actions);

    return true;
}


/****************************************************************************
*
*   DbData - Samples
*
***/

//===========================================================================
pgno_t DbData::updateSampleIndexRoot(
    DbTxn & txn,
    pgno_t spno,
    unsigned rootId,
    pgno_t pgno
) {
    auto sp = txn.pin<SamplePage>(spno);
    assert(rootId == sp->hdr.id);
    auto oldRoot = sp->sampleIndex;
    txn.walSampleUpdateIndexRoot(spno, pgno);
    return oldRoot;
}

//===========================================================================
bool DbData::findLastSamplePage(
    DbTxn & txn,
    pgno_t * spno,
    uint32_t id,
    bool createIfNotExists
) {
    scoped_lock lk{m_mndxMut};
    DbTxn::PinScope pins(txn);

    if (radixFind(txn, spno, m_sampleRoot, id))
        return true;
    if (createIfNotExists) {
        // No pages, create page and add sample to it.
        *spno = allocPgno(txn);
        radixInsert(txn, m_sampleRoot, id, *spno);
        pins.keep(*spno);
    }
    return false;
}

//===========================================================================
bool DbData::findSamplePage(
    DbTxn & txn,
    pgno_t * spno,
    pgno_t root,
    uint32_t id,
    TimePoint time
) {
    if (!root || root == pgno_t::npos) {
        *spno = {};
        return false;
    }

    SampleIndexRec rec = {
        .time = time,
        .pgno = {},
    };
    auto key = ::trieKey(rec);
    DbSamplePageHeap heap(&txn, this, root);
    StrTrieBase trie(&heap);
    auto i = trie.findLessEqual(key);
    auto found = i ? *i : trie.front();
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
void DbData::updateSampleIndex(
    DbTxn & txn,
    const SamplePage * sp,
    pgno_t spno,
    optional<TimePoint> oldTime,
    optional<TimePoint> newTime,
    optional<Duration> expiration
) {
    if (oldTime == newTime)
        return;

    DbSamplePageHeap heap(
        &txn,
        this,
        sp->sampleIndex,
        sp->hdr.id,
        sp->hdr.pgno
    );
    StrTrieBase trie(&heap);
    [[maybe_unused]] auto result = false;
    if (oldTime) {
        assert(sp->sampleIndex);
        auto key = ::trieKey({*oldTime, spno});
        result = trie.erase(key);
        assert(result);
    }

    if (!newTime)
        return;
    auto key = ::trieKey({*newTime, spno});
    result = trie.insert(key);
    assert(result);

    if (!expiration)
        return;
    key = ::trieKey({*newTime - *expiration});
    auto i = trie.findLess(key);
    if (!i) {
        logMsgFatal() << "updateSampleIndex(" << sp->hdr.id << ", "
            << time << "): sample index with closed lower bound";
        return;
    }
    SampleIndexRec rec;
    if (!::parseTrieKey(&rec, *i)) {
        logMsgFatal() << "updateSampleIndex(" << sp->hdr.id << ", "
            << time << "): invalid entry in sample index";
        return;
    }
    if (empty(rec.time)) {
        // Oldest unexpired sample would land in the first page, so there are
        // no completely expired pages to discard.
        return;
    }
    //trie.erase(
}

//===========================================================================
void DbData::eraseSamples(DbTxn & txn, uint32_t id) {
    pgno_t spno = {};
    {
        scoped_lock lk{m_mndxMut};
        DbTxn::PinScope pins(txn);
        spno = radixSwapValue(txn, m_sampleRoot, id, {});
    }
    if (!spno)
        return;

    auto sp = txn.pin<SamplePage>(spno);
    auto iroot = sp->sampleIndex;
    if (!iroot) {
        freePage(txn, spno);
        return;
    }

    trieVisitWithPrefix(txn, iroot, {}, [this](auto & txn, auto & key) {
        SampleIndexRec rec;
        if (!::parseTrieKey(&rec, key)) {
            assert(!"Bad sample index entry");
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

namespace {

// unexpired
// split point
// replacement point
// split point within replacement bits

struct SampleUpdateState {
    const DbSample sample = {};
    TimePoint firstTime = {};
    DbUnpackIter in;

    // Modification
    DbPack pack;
    TimePoint lastTime = {};
    size_t updLen = {};
    size_t replPos = {};
    size_t replLen = {};
    size_t truncPos = {};

    // Second page of split
    DbPack pack2;
    DbSample firstSample2 = {};
    TimePoint lastTime2 = {};

    SampleUpdateState(
        const DbSample & s,
        span<uint8_t> tmp,
        span<uint8_t> tmp2,
        const void * data,
        size_t dataBits,
        TimePoint firstTime,
        TimePoint lastTime
    );
    void rewind();
};
} // namespace

//===========================================================================
SampleUpdateState::SampleUpdateState(
    const DbSample & s,
    span<uint8_t> tmp,
    span<uint8_t> tmp2,
    const void * data,
    size_t dataBits,
    TimePoint firstTime,
    TimePoint lastTime
)
    : sample(s)
    , firstTime(firstTime)
    , in(data, dataBits, 0, { .sample = { firstTime } })
    , pack(tmp.data(), tmp.size(), 0, { .sample = { firstTime } })
    , lastTime(lastTime)
    , pack2(tmp2.data(), tmp2.size())
{}

//===========================================================================
void SampleUpdateState::rewind() {
    in.seek(0, { .sample = { firstTime } });
    pack.retarget(pack.data(), pack.capacity());
    lastTime = {};
    updLen = replPos = replLen = 0;
    pack2.retarget(pack2.data(), pack2.capacity());
    firstSample2 = {};
    lastTime2 = {};
}

//===========================================================================
static void packSample(SampleUpdateState * sus, const DbSample & sample) {
    if (!sus->pack.put(sample))
        logMsgFatal() << "Sample page too large for just one split.";
}

//===========================================================================
static void packSample2(SampleUpdateState * sus, const DbSample & sample) {
    if (!sus->pack2.put(sample))
        logMsgFatal() << "Sample page too large for just one split.";
}

//===========================================================================
// Returns true if the sample is included in the calculated update, false if
// it's just the removal of expired samples and does not overlap with the new
// sample.
static bool calcExpiredSamples(
    SampleUpdateState * sus,
    const DbMetricInfo & mi
) {
    auto firstSampleTime = sus->sample.time - mi.retention;
    for (; sus->in; ++sus->in) {
        sus->replLen = sus->in.spos();
        if (sus->in->time >= firstSampleTime)
            break;
    }
    if (!sus->in) {
        packSample(sus, sus->sample);
        sus->firstTime = sus->sample.time;
        sus->lastTime = sus->sample.time;
        sus->replLen = sus->in.bits();
        sus->updLen = sus->pack.bits();
        return true;
    }
    if (sus->sample.time > sus->in->time) {
        sus->firstTime = sus->in->time;
        packSample(sus, *sus->in);
    } else {
        sus->firstTime = sus->sample.time;
        packSample(sus, sus->sample);
        if (sus->sample.time == sus->in->time) {
            ++sus->in;
            if (!sus->in)
                sus->lastTime = sus->sample.time;
        }
    }
    for (; sus->in; ++sus->in) {
        packSample(sus, *sus->in);
        sus->replLen = sus->in.spos();
        if (sus->pack.state() == sus->in.state())
            break;
    }
    sus->updLen = sus->pack.bits();
    return !sus->in || sus->in->time > sus->sample.time;
}

//===========================================================================
static void calcSampleUpdate(SampleUpdateState * sus) {
    assert(!sus->in || sus->firstTime == sus->in->time);
    for (; sus->in; ++sus->in) {
        if (sus->in->time >= sus->sample.time)
            break;
        packSample(sus, *sus->in);
    }
    sus->replPos = sus->pack.bits();
    if (sus->in && sus->in->time == sus->sample.time) {
        if (sus->in->value == sus->sample.value)
            return;
        ++sus->in;
    }
    packSample(sus, sus->sample);
    for (; sus->in; ++sus->in) {
        packSample(sus, *sus->in);
        if (sus->pack.state() == sus->in.state())
            break;
    }
    sus->replLen = sus->in.spos() - sus->replPos;
    sus->updLen = sus->pack.bits() - sus->replPos;
}

//===========================================================================
static void calcSamplePageSplit(SampleUpdateState * sus, size_t keepBits) {
    assert(keepBits < sus->in.bits() + sus->updLen - sus->replLen);

    if (keepBits > sus->pack.bits()) {
        // Split in area after update.
        auto inKeepPoint = keepBits - sus->updLen + sus->replLen;
        for (;;) {
            ++sus->in;
            if (sus->in.spos() + sus->in.slen() > inKeepPoint)
                break;
            packSample(sus, *sus->in);
        }
        sus->lastTime = sus->pack.state().sample.time;
        sus->truncPos = sus->in.spos() + sus->updLen - sus->replLen;

        sus->firstSample2 = *sus->in;
        sus->pack2.retarget(
            sus->pack2.data(),
            sus->pack2.capacity(),
            0,
            { .sample = { .time = sus->firstSample2.time } }
        );
        for (; sus->in; ++sus->in) {
            packSample2(sus, *sus->in);
        }
        sus->lastTime2 = sus->pack2.state().sample.time;
        return;
    }

    DbUnpackIter inpack(
        sus->pack.data(),
        sus->pack.bits(),
        0,
        { .sample = { .time = sus->firstTime } }
    );
    size_t splitPos = 0;
    auto splitLastTime = inpack->time;
    for (; inpack; ++inpack) {
        splitPos = inpack.spos();
        if (splitPos + inpack.slen() > keepBits)
            break;
        splitLastTime = inpack->time;
    }
    sus->lastTime = splitLastTime;
    sus->firstSample2 = *inpack;
    sus->pack2.retarget(
        sus->pack2.data(),
        sus->pack2.capacity(),
        0,
        { .sample = { .time = sus->firstSample2.time } }
    );
    for (; inpack; ++inpack) {
        packSample2(sus, *inpack);
    }
    for (; sus->in; ++sus->in) {
        packSample2(sus, *sus->in);
    }
    sus->lastTime2 = sus->pack2.state().sample.time;

    if (splitPos >= sus->pack.bits() - sus->updLen) {
        // Split takes place at or in update.
        sus->updLen = splitPos - sus->replPos;
        sus->replLen = sus->in.bits() - sus->replPos;
    } else {
        // Split in area before update.
        sus->replLen = 0;
        sus->updLen = 0;
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
    if (!findLastSamplePage(txn, &spno, id, true)) {
        // No existing samples, new page was allocated (and pinned). Initialize
        // it and add this new sample.
        txn.walSampleInit(spno, id, mi.type, time, value);
        s_perfAdd += 1;
        return;
    }
    auto sp = txn.pin<SamplePage>(spno);
    bool spIsLastPage = true;
    if (time >= sp->firstTime) {
        // Updating sample on last page.
        assert(spno);
    } else {
        // Sample older than last page.
        auto firstSampleTime = sp->lastTime - mi.retention;
        if (time < firstSampleTime) {
            // Sample older than retention period, ignore it.
            s_perfAncient += 1;
            return;
        }
        // Search sample index for containing page.
        pgno_t pgno;
        if (!findSamplePage(txn, &pgno, sp->sampleIndex, id, time)) {
            // No (or malformed) sample index, add to last page.
            assert(spno);
        } else {
            // Update sample on page found in index.
            spno = pgno;
            sp = txn.pin<SamplePage>(spno);
            spIsLastPage = false;
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
    auto dataLen = sampleDataPerPage(sp->sampleType, m_pageSize);
    auto buf = (uint8_t *) mallocAuto(3 * dataLen);
    Finally finBuf([&buf]() { freeAuto(buf); });
    SampleUpdateState sus(
        { .time = time, .value = value },
        { buf, 2 * dataLen },
        { buf + 2 * dataLen, dataLen },
        sp->data,
        sp->dataBits,
        sp->firstTime,
        sp->lastTime
    );
    calcSampleUpdate(&sus);

    if (sus.in.bits() - sus.replLen + sus.updLen <= 8 * dataLen) {
        // Original page plus update still fits on one page.

        if (!sus.replLen && !sus.updLen) {
            // Nothing to remove and nothing to add.
            s_perfDup += 1;
            return;
        }

        auto oldTime = sp->firstTime;
        txn.walSampleReplace(
            spno,
            sus.replPos,
            sus.replLen,
            sus.pack.data(),
            sus.updLen
        );
        TimePoint last = {};
        if (time > sp->lastTime)
            last = time;
        if (!sus.replPos) {
            assert(time >= sp->firstTime);
            txn.walSampleUpdateTime(spno, time, last);
            updateSampleIndex(txn, sp, spno, oldTime, sp->firstTime);
        } else if (!empty(last)) {
            txn.walSampleUpdateTime(spno, {}, last);
        }
        s_perfAdd += 1;
        return;
    }

    //-----------------------------------------------------------------------
    // Not enough room left on page for update.

    if (time > sp->firstTime + mi.retention) {
        // Page has expired entries, remove them. Already checked that not all
        // samples are expired, so some current samples will remain.
        sus.rewind();
        calcExpiredSamples(&sus, mi);
        auto oldTime = sp->firstTime;
        txn.walSampleReplace(
            spno,
            sus.replPos,
            sus.replLen,
            sus.pack.data(),
            sus.updLen
        );
        txn.walSampleUpdateTime(spno, sus.firstTime, sus.lastTime);
        updateSampleIndex(txn, sp, spno, oldTime, sp->firstTime);

        // Now, with a little more space on the page, try again.
        if (sus.firstTime == time && sus.lastTime == time) {
            // Unless no update is needed because the entire page was just
            // replaced with the new sample.
            return;
        }
        updateSample(txn, id, time, value);
        return;
    }

    // Update/create sample index.
    if (!sp->sampleIndex) {
        // Sample index doesn't already exist, add current page.
        updateSampleIndex(txn, sp, spno, {}, sp->firstTime);
    }

    if (spIsLastPage && time > sp->lastTime) {
        // Sample belongs at end of last page. Add entirely new page with just
        // the new sample.
        auto spno2 = allocPgno(txn);
        txn.walSampleInit(spno2, id, mi.type, time, value);
        s_perfAdd += 1;

        // Add new page to sample index.
        updateSampleIndex(txn, sp, spno2, {}, time, mi.retention);
        return;
    }

    // Split samples onto two pages.
    auto keepBits = sus.in.bits() + sus.updLen - sus.replLen;
    Duration retention = {};
    if (spIsLastPage) {
        keepBits = keepBits * 7 / 8;
        retention = mi.retention;
    } else {
        keepBits /= 2;
    }
    calcSamplePageSplit(&sus, keepBits);

    // Write new following page.
    auto spno2 = allocPgno(txn);
    txn.walSampleInit(
        spno2,
        id,
        mi.type,
        sus.firstSample2.time,
        sus.firstSample2.value
    );
    txn.walSampleUpdateTime(spno2, {}, sus.lastTime2);
    auto sp2 = txn.pin<SamplePage>(spno2);
    txn.walSampleReplace(
        spno2,
        sp2->dataBits,
        0,
        sus.pack2.data(),
        sus.pack2.bits() - sp2->dataBits
    );
    updateSampleIndex(txn, sp2, spno2, {}, sus.firstSample2.time, retention);

    // Update existing page.
    txn.walSampleUpdateTime(spno, {}, sus.lastTime);
    if (sus.truncPos) {
        txn.walSampleReplace(
            spno,
            sus.truncPos,
            sp->dataBits - sus.truncPos,
            nullptr,
            0
        );
    }
    txn.walSampleReplace(
        spno,
        sus.replPos,
        sus.replLen,
        sus.pack.data(),
        sus.updLen
    );
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
    sp->sampleIndex = {};
    sp->sampleType = type;

    // Write value to sp->data[]
    DbPack pack(sp->data, sampleDataPerPage(type, m_pageSize));
    pack.put(time, value);
    sp->dataBits = (uint16_t) (pack.bits());
}

//===========================================================================
void DbData::onWalApplySampleUpdateRoot(
    void * ptr,
    pgno_t rootPage
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
    sp->sampleIndex = rootPage;
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
void DbData::onWalApplySampleReplace(
    void * ptr,
    size_t dstPos,
    size_t dstBits,
    const uint8_t * src,
    size_t srcBits
) {
    auto sp = static_cast<SamplePage *>(ptr);
    assert(sp->hdr.type == sp->kPageType);
    auto newCount = sp->dataBits + srcBits - dstBits;
    if (srcBits <= dstBits) {
        assert(dstBits - srcBits < sp->dataBits);
        BitSpan bits(sp->data, sp->dataBits);
        bits.replace(dstPos, dstBits, src, 0, srcBits);
    } else {
        assert(newCount <=
            sampleDataPerPage(sp->sampleType, m_pageSize) * sizeof *sp->data);
        BitSpan bits(sp->data, newCount);
        bits.replace(dstPos, dstBits, src, 0, srcBits);
    }
    sp->dataBits = (uint16_t) newCount;
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
