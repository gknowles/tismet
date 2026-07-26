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
#if __cpp_constexpr >= 202207
constexpr BitSpan sampleDataSpan(DbData::SamplePage * sp, size_t pageSize) {
#else
inline static BitSpan sampleDataSpan(
    DbData::SamplePage * sp,
    size_t pageSize
) {
#endif
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
    const uint8_t kVersion = 0;
    string key;
    auto len = sizeof id + sizeof kVersion
        + info.name.size()
        + sizeof info.creation
        + sizeof info.type
        + sizeof info.retention + sizeof info.interval
        + sizeof info.lastInfoWrite;
    key.resize(len);
    auto ptr = reinterpret_cast<std::byte *>(key.data());
    hton32(&ptr, id);
    *ptr++ = static_cast<std::byte>(kVersion);
    memcpy(ptr, info.name.data(), info.name.size());
    ptr += info.name.size();
    hton64(&ptr, info.creation.time_since_epoch().count());
    *ptr++ = static_cast<std::byte>(info.type);
    hton64(&ptr, info.retention.count());
    hton64(&ptr, info.interval.count());
    hton64(&ptr, info.lastInfoWrite.time_since_epoch().count());
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
    const uint8_t kVersion = 0;
    auto minLen = sizeof *id + sizeof kVersion
        + sizeof out->creation
        + sizeof out->type
        + sizeof out->retention + sizeof out->interval
        + sizeof out->lastInfoWrite;
    if (val.size() < minLen) {
        *id = 0;
        *out = {};
        return false;
    }
    auto base = reinterpret_cast<const std::byte *>(val.data());
    auto ptr = base;
    *id = ntoh32(&ptr);
    auto ver = ntoh8(&ptr);
    if (!*id || ver != kVersion) {
        *out = {};
        return false;
    }
    out->name = val.substr(ptr - base, val.size() - minLen);
    ptr += out->name.size();
    out->creation = TimePoint(Duration(ntoh64(&ptr)));
    out->type = static_cast<DbSampleType>(ntoh8(&ptr));
    out->retention = Duration(ntoh64(&ptr));
    out->interval = Duration(ntoh64(&ptr));
    out->lastInfoWrite = TimePoint(Duration(ntoh64(&ptr)));
    assert(ptr == reinterpret_cast<const std::byte *>(
        val.data() + val.size())
    );
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
    DbMetricInfo info = mi;
    if (!empty(from.retention))
        info.retention = from.retention;
    if (!empty(from.interval))
        info.interval = from.interval;
    if (from.type)
        info.type = from.type;
    if (!empty(from.creation))
        info.creation = from.creation;
    if (mi == info)
        return;
    info.lastInfoWrite = timeNow();

    // Update indexes
    vector<TrieAction> actions = {
        { TrieAction::kInsert, txn.roots().info, trieKey(id, info) },
        { TrieAction::kErase, txn.roots().info, trieKey(id, mi) },
    };
    trieApply(txn, actions);

    // Remove all existing samples
    eraseSamples(txn, id);
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

    // update in memory references
    m_numMetrics -= 1;

    return true;
}


/****************************************************************************
*
*   DbData - Samples
*
***/

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
        // No pages, create page and add it to metric samples index.
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
        .pgno = pgno_t::npos,
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


/****************************************************************************
*
*   DbData - Erase Samples
*
***/

//===========================================================================
void DbData::eraseSamples(DbTxn & txn, uint32_t id) {
    auto spno = updateLastSamplePage(txn, id, {});
    if (!spno)
        return;

    auto sp = txn.pin<SamplePage>(spno);
    auto iroot = sp->sampleIndex;
    assert(iroot);
    if (iroot == npos) {
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


/****************************************************************************
*
*   DbData - Update Samples
*
***/

//===========================================================================
pgno_t DbData::updateLastSamplePage(
    DbTxn & txn,
    uint32_t id,
    pgno_t spno
) {
    scoped_lock lk{m_mndxMut};
    DbTxn::PinScope pins(txn);
    return radixSwapValue(txn, m_sampleRoot, id, spno);
}

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
    if (oldRoot == pgno)
        return oldRoot;
    txn.walSampleUpdateIndexRoot(spno, pgno);
    return oldRoot;
}

//===========================================================================
static pgno_t clearSampleIndexRoot(
    DbTxn & txn,
    DbData & data,
    const DbData::SamplePage * root
) {

    if (root->hdr.type == DbPageType::kFree) {
        // No need to clear as the page has already been freed. Presumably by
        // index update as an expired page.
        return npos;
    } else {
        return data.updateSampleIndexRoot(
            txn,
            root->hdr.pgno,
            root->hdr.id,
            npos
        );
    }
}

//===========================================================================
// Note: Any updates to sp->firstTime MUST be made before updating the index.
// The index is updated based on the values of:
//      sp->hdr.pgno
//      sp->firstTime
//      oldTime
//      expiration
void DbData::updateSampleIndex(
    DbTxn & txn,
    const SamplePage * root,
    pgno_t sampleIndex,
    const SamplePage * sp,  // spno & newTime
    optional<TimePoint> oldTime,
    optional<Duration> expiration
) {
    assert(sampleIndex);
    if (oldTime == sp->firstTime)
        return;

    DbSamplePageHeap heap(
        &txn,
        this,
        sampleIndex,
        root->hdr.id,
        root->hdr.pgno
    );
    StrTrieBase trie(&heap);
    [[maybe_unused]] auto result = false;
    if (oldTime) {
        assert(sampleIndex != npos);
        auto key = ::trieKey({*oldTime, sp->hdr.pgno});
        result = trie.erase(key);
        assert(result);
    }

    auto key = ::trieKey({sp->firstTime, sp->hdr.pgno});
    result = trie.insert(key);
    assert(result);

    if (!expiration)
        return;

    //-----------------------------------------------------------------------
    // Remove pages that hold nothing but expired samples.
    auto firstSampleTime = sp->firstTime - *expiration;

    // Find first page that might have unexpired samples.
    key = ::trieKey({sp->firstTime - *expiration, pgno_t::npos});
    auto i = trie.findLessEqual(key);
    if (!i) {
        // No definitely expired pages
        return;
    }
    SampleIndexRec rec;
    if (!::parseTrieKey(&rec, *i)) {
        logMsgFatal() << "updateSampleIndex(" << sp->hdr.id << ", "
            << time << "): invalid entry in sample index";
        return;
    }
    auto upperbound = rec.time;

    // Remove definitely completely expired pages.
    for (;;) {
        auto i = trie.begin();
        if (!::parseTrieKey(&rec, *i)) {
            logMsgFatal() << "updateSampleIndex(" << sp->hdr.id << ", "
                << time << "): invalid entry in sample index";
            return;
        }
        if (rec.time == upperbound) {
            // All expired pages have been erased.
            break;
        }
        freePage(txn, rec.pgno);
        result = trie.erase(*i);
        assert(result);
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
    TimePoint lastTime = {};
    DbUnpackIter in;

    // Modification
    DbPack pack;

    // If there is a trunc it happens before the replace (if any)
    size_t truncPos = {};

    size_t updPos = {};
    size_t updLen = {};
    size_t replPos = {};
    size_t replLen = {};

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
    void rewind(TimePoint firstTime, TimePoint lastTime);
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
    , lastTime(lastTime)
    , in(data, dataBits, 0, firstTime)
    , pack(tmp.data(), tmp.size(), 0, firstTime)
    , pack2(tmp2.data(), tmp2.size())
{}

//===========================================================================
void SampleUpdateState::rewind(TimePoint first, TimePoint last) {
    firstTime = first;
    DbPackState st = {.sample = { firstTime }};
    in.seek(0, st);
    pack.retarget(0, st);
    lastTime = last;
    updPos = updLen = 0;
    replPos = replLen = 0;
    truncPos = 0;
    pack2.retarget(0, {});
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
    auto lastSampleTime = max(sus->sample.time, sus->lastTime);
    auto firstSampleTime = lastSampleTime - mi.retention;
    for (; sus->in; ++sus->in) {
        if (sus->in->time >= firstSampleTime)
            break;
    }
    if (!sus->in) {
        // All existing samples expired, completely replace with new sample.
        sus->replLen = sus->in.bits();
        sus->firstTime = sus->sample.time;
        sus->lastTime = sus->sample.time;
        sus->pack.retarget(0, sus->firstTime);
        packSample(sus, sus->sample);
        sus->updLen = sus->pack.bits();
        return true;
    }
    if (sus->in->time < sus->sample.time) {
        sus->firstTime = sus->in->time;
        sus->pack.retarget(0, sus->firstTime);
        for (;;) {
            packSample(sus, *sus->in);
            if (sus->pack.state() == sus->in.state()
                || !++sus->in
            ) {
                sus->replLen = sus->in.epos();
                sus->updLen = sus->pack.bits();
                return false;
            }
            if (sus->in->time >= sus->sample.time) {
                packSample(sus, sus->sample);
                if (sus->in->time == sus->sample.time)
                    ++sus->in;
                break;
            }
        }
    } else if (sus->in->time == sus->sample.time) {
        sus->firstTime = sus->sample.time;
        sus->pack.retarget(0, sus->firstTime);
        packSample(sus, sus->sample);
        ++sus->in;
    } else {
        logMsgFatal() << "Expired sample calculation thinks update expired.";
    }
    for (; sus->in; ++sus->in) {
        auto & s = *sus->in;
        packSample(sus, s);
        if (sus->pack.state() == sus->in.state())
            break;
    }
    sus->replLen = sus->in.epos();
    sus->updLen = sus->pack.bits();
    return true;
}

//===========================================================================
// sus inputs:
//  sample {.time, .value}
//  firstTime
//  lastTime
//  in
// sus outputs - only stable if updLen and/or replLen are non-zero:
//  firstTime - if sample.time < firstTime
//  lastTime - if sample.time > lastTime
//  in - advanced to end, data unchanged
//  pack - data of page with sample added (may be too large to fit on page)
//  updPos - portion of pack (new data) to add
//  updLen
//  replPos - portion of in (old data) to replace
//  replLen
static void calcSampleUpdate(SampleUpdateState * sus) {
    assert(!sus->in || sus->firstTime == sus->in->time);
    assert(!sus->replLen && !sus->updLen);
    for (; sus->in; ++sus->in) {
        auto & s = *sus->in;
        if (s.time >= sus->sample.time)
            break;
        packSample(sus, s);
    }
    if (!sus->in) {
        sus->lastTime = sus->sample.time;
    } else {
        if (sus->in->time == sus->sample.time) {
            if (sus->in->value == sus->sample.value)
                return;
            ++sus->in;
        } else {
            assert(sus->in->time > sus->sample.time);
            if (!sus->in.spos()) {
                // Inserting sample with new lower first time, so set packing
                // state to start with new first time.
                sus->firstTime = sus->sample.time;
                sus->pack.retarget(0, sus->firstTime);
            }
        }
    }
    packSample(sus, sus->sample);
    for (; sus->in; ++sus->in) {
        auto & s = *sus->in;
        packSample(sus, s);
    }
    auto head = BitSpan::mismatch(
        sus->in.data(),
        0,
        sus->in.bits(),
        sus->pack.data(),
        0,
        sus->pack.bits()
    );
    auto tail = BitSpan::rmismatch(
        sus->in.data(),
        head,
        sus->in.bits() - head,
        sus->pack.data(),
        head,
        sus->pack.bits() - head
    );
    auto common = head + tail;
    assert(common <= sus->in.bits());
    assert(common <= sus->pack.bits());
    sus->updPos = head;
    sus->updLen = sus->pack.bits() - common;
    sus->replPos = head;
    sus->replLen = sus->in.epos() - common;
}

//===========================================================================
// keepBits - bits to keep on old page, will be adjusted to sample alignment.
// sus inputs:
//  firstTime
//  in (.bits)
//  pack
//  updPos
//  updLen
//  replPos
//  replLen
// sus outputs - first page
//  firstTime
//  lastTime
//  truncPos
//  updPos
//  updLen
//  replPos
//  replLen
// sus outputs - second page
//  pack2
//  firstSample2
//  lastTime2
static void calcSamplePageSplit(SampleUpdateState * sus, size_t keepBits) {
    assert(sus->pack.bits() == sus->in.bits() + sus->updLen - sus->replLen);
    assert(sus->pack.bits() > keepBits);
    assert(!sus->truncPos);

    // Find split point.
    DbUnpackIter inpack(
        sus->pack.data(),
        sus->pack.bits(),
        0,
        sus->firstTime
    );
    for (;; ++inpack) {
        assert(inpack);
        if (inpack.epos() > keepBits)
            break;
    }
    auto splitPos = inpack.spos();
    sus->firstSample2 = *inpack;
    sus->pack2.retarget(
        sus->pack2.data(),
        sus->pack2.capacity(),
        0,
        sus->firstSample2.time
    );
    for (;;) {
        auto & s = *inpack;
        packSample2(sus, s);
        if (!++inpack)
            break;
    }
    sus->lastTime2 = sus->pack2.state().sample.time;

    if (splitPos <= sus->updPos) {
        // Split in area before update.
        sus->truncPos = splitPos;
        sus->replLen = 0;
        sus->updLen = 0;
    } else if (splitPos <= sus->updPos + sus->updLen) {
        // Split takes place at or in update.
        sus->truncPos = 0;
        sus->replLen = sus->in.bits() - sus->replPos;
        sus->updLen = splitPos - sus->updPos;
    } else {
        // Split in area after update.
        sus->truncPos = splitPos - sus->updLen + sus->replLen;
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
        updateLastSamplePage(txn, id, spno);
        s_perfAdd += 1;
        return;
    }
    auto sp = txn.pin<SamplePage>(spno);
    // Remember this last page as spLast, it stores the sampleIndex root which
    // we may need later.
    auto spLast = sp;

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
        if (!findSamplePage(txn, &pgno, spLast->sampleIndex, id, time)) {
            // No (or malformed?) sample index, add to last (and only) page.
            assert(spno);
        } else {
            // Select page found in index (and we know it's not the last page
            // of the index) to receive the sample.
            spno = pgno;
            sp = txn.pin<SamplePage>(spno);
        }
    }

    //-----------------------------------------------------------------------
    if (time > spLast->lastTime + mi.retention) {
        // The new sample is far enough in the future that all preexisting
        // samples become expired. Remove all samples and add as new initial
        // sample.
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
    memset(buf, 0, 3 * dataLen);
    auto tmp = span<uint8_t>(buf, 2 * dataLen);
    auto tmp2 = span<uint8_t>(buf + 2 * dataLen, dataLen);
    SampleUpdateState sus(
        {.time = time, .value = value},
        tmp,
        tmp2,
        sp->data,
        sp->dataBits,
        sp->firstTime,
        sp->lastTime
    );
    calcSampleUpdate(&sus);

    if (!sus.replLen && !sus.updLen) {
        // Nothing to remove and nothing to add.
        s_perfDup += 1;
        return;
    }
    if (sus.pack.bits() <= 8 * dataLen) {
        // Original page plus update still fits on one page.

        txn.walSampleReplace(
            spno,               // dst
            sus.replPos,
            sus.replLen,
            sus.pack.data(),    // src
            sus.updPos,
            sus.updLen
        );
        if (time > sp->lastTime) {
            txn.walSampleUpdateTime(spno, {}, time);
        } else if (time >= sp->firstTime) {
            // New sample is within [firstTime, lastTime]; no update to
            // firstTime or lastTime.
        } else {
            assert(time < sp->firstTime);
            auto oldTime = sp->firstTime;
            txn.walSampleUpdateTime(spno, time, {});
            if (auto si = spLast->sampleIndex; si != npos) {
                updateSampleIndex(txn, spLast, si, sp, oldTime);
            } else {
                // There is only one page, otherwise there would be an
                // index of the pages.
            }
        }
        s_perfAdd += 1;
        return;
    }

    //-----------------------------------------------------------------------
    // Not enough room left on page for update.

    if (time > sp->firstTime + mi.retention) {
        // Page has expired entries, remove them and try again. Already checked
        // that not all samples are expired, so some current samples will
        // remain.
        sus.rewind(sp->firstTime, sp->lastTime);
        calcExpiredSamples(&sus, mi);
        txn.walSampleReplace(
            spno,               // dst
            sus.replPos,
            sus.replLen,
            sus.pack.data(),    // src
            sus.replPos,
            sus.updLen
        );
        assert(sus.firstTime != sp->firstTime);
        auto oldTime = sp->firstTime;
        if (sus.lastTime != sp->lastTime) {
            txn.walSampleUpdateTime(spno, sus.firstTime, sus.lastTime);
        } else {
            txn.walSampleUpdateTime(spno, sus.firstTime, {});
        }
        if (auto si = spLast->sampleIndex; si != npos) {
            updateSampleIndex(txn, spLast, si, sp, oldTime);
        } else {
            // There is no index so there must only be the one page.
        }

        // Now, with a little more space on the page, try again.
        if (sus.firstTime == time && sus.lastTime == time) {
            // Retry isn't needed because the entire page was just replaced
            // with the new sample.
            return;
        }
        updateSample(txn, id, time, value);
        return;
    }

    // Ensure sample index exists.
    if (spLast->sampleIndex == npos) {
        // Sample index doesn't already exist, create it with an entry for
        // the current page.
        assert(sp == spLast);
        updateSampleIndex(txn, spLast, spLast->sampleIndex, sp, {});
    }

    if (sp == spLast && time > sp->lastTime) {
        // Sample belongs at end of last page. Add entirely new page with just
        // the new sample.
        auto spno2 = allocPgno(txn);
        txn.walSampleInit(spno2, id, mi.type, time, value);
        updateLastSamplePage(txn, id, spno2);
        s_perfAdd += 1;

        // Add new page to sample index.
        auto sp2 = txn.pin<SamplePage>(spno2);
        updateSampleIndex(
            txn,
            sp2,
            spLast->sampleIndex,
            sp2,
            {},
            mi.retention
        );
        clearSampleIndexRoot(txn, *this, sp);
        return;
    }

    // Split samples into two pages.
    auto keepBits = sus.pack.bits();
    Duration retention = {};
    if (sp == spLast) {
        keepBits = keepBits * 7 / 8;
        retention = mi.retention;
    } else {
        keepBits /= 2;
    }
    calcSamplePageSplit(&sus, keepBits);

    // Write new following page.
    auto && s2 = sus.firstSample2;
    auto spno2 = allocPgno(txn);
    txn.walSampleInit(spno2, id, mi.type, s2.time, s2.value);
    txn.walSampleUpdateTime(spno2, {}, sus.lastTime2);
    auto sp2 = txn.pin<SamplePage>(spno2);
    txn.walSampleReplace(
        spno2,              // dst
        sp2->dataBits,
        0,
        sus.pack2.data(),   // src
        sp2->dataBits,
        sus.pack2.bits() - sp2->dataBits
    );
    if (sp == spLast) {
        // Split from last page into new last page.
        updateLastSamplePage(txn, id, spno2);
        updateSampleIndex(txn, sp2, spno2, sp2, {}, retention);
        clearSampleIndexRoot(txn, *this, sp);
    } else {
        // Split doesn't effect last page.
        updateSampleIndex(txn, spLast, spLast->sampleIndex, sp2, {});
    }

    // Update existing page.
    if (sus.firstTime < sp->firstTime) {
        auto oldTime = sp->firstTime;
        txn.walSampleUpdateTime(spno, sus.firstTime, sus.lastTime);
        updateSampleIndex(txn, spLast, spLast->sampleIndex, sp, oldTime);
    } else {
        assert(sus.firstTime == sp->firstTime);
        txn.walSampleUpdateTime(spno, {}, sus.lastTime);
    }
    if (sus.truncPos) {
        txn.walSampleReplace(
            spno,           // dst
            sus.truncPos,
            sp->dataBits - sus.truncPos,
            nullptr,        // src
            0,
            0
        );
    }
    if (sus.replLen || sus.updLen) {
        txn.walSampleReplace(
            spno,               // dst
            sus.replPos,
            sus.replLen,
            sus.pack.data(),    // src
            sus.replPos,
            sus.updLen
        );
    }
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
    sp->sampleIndex = npos;
    sp->sampleType = type;

    // Write value to sp->data[]
    DbPack pack(
        sp->data,
        sampleDataPerPage(type, m_pageSize),
        0,
        time
    );
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
    if (!empty(firstTime))
        sp->firstTime = firstTime;
    if (!empty(lastTime))
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
    size_t bitCnt = sp->dataBits;
    if (srcBits <= dstBits) {
        // Shrinking.
        assert(dstBits - srcBits <= sp->dataBits);
    } else {
        // Growing, but not too much.
        assert(newCount <= 8 * sampleDataPerPage(sp->sampleType, m_pageSize));
        bitCnt = newCount;
    }
    assert(dstBits <= sp->dataBits);
    if (dstPos == -1) {
        // Special value indicates operation is at end of data.
        dstPos = sp->dataBits - dstBits;
    }
    assert(dstPos + dstBits <= sp->dataBits);
    auto wordBits = BitSpan::kWordBits;
    auto words = (bitCnt + wordBits - 1) / wordBits;
    BitSpan bits(sp->data, words);
    if (!srcBits) {
        bits.erase(dstPos, dstBits);
    } else if (dstPos + dstBits == sp->dataBits) {
        bits.set(dstPos, src, 0, srcBits);
    } else {
        bits.replace(dstPos, dstBits, src, 0, srcBits);
    }
    sp->dataBits = (uint16_t) newCount;
}


/****************************************************************************
*
*   DbData - Get Samples
*
***/

namespace {

enum GetSampleResult {
    kAborted,   // Callback handler returned false.
    kMore,      // More matching samples may be on next page.
    kComplete,  // All matching samples have been reported.
};

} // namespace

//===========================================================================
static GetSampleResult reportSamples(
    unsigned * count,
    IDbDataNotify * notify,
    DbTxn & txn,
    const DbMetricInfo & mi,
    const DbData::SamplePage * sp,
    TimePoint first,
    TimePoint last
) {
    assert(sp->hdr.type == sp->kPageType);
    DbUnpackIter in(
        sp->data,
        sp->dataBits,
        0,
        sp->firstTime
    );
    if (*count) {
        assert(in);
        for (;;) {
            if (in->time > last)
                return kComplete;
            *count += 1;
            if (!notify->onDbSample(sp->hdr.id, in->time, in->value))
                return kAborted;
            if (!++in)
                return kMore;
        }
    }

    // Still searching for first value in [first, last].
    if (sp->lastTime < first) {
        // The range starts in the gap between this page and the next.
        if (first == last) {
            // And ends in the gap, therefore there are no samples.
            return kComplete;
        }
        // The range may extend onto the next page, so we have to check it.
        return kMore;
    }

    if (sp->firstTime > last) {
        // This can occur when there is only a single sample page and no index.
        // Otherwise the index search (via findLessEqual) precludes starting on
        // a page that's beyond the end of the range.
        return kComplete;
    }
    for (;; ++in) {
        assert(in);
        if (in->time >= first)
            break;
    }
    if (in->time > last) {
        // First sample after range start was also after range end.
        return kComplete;
    }
    DbSeriesInfo dsi;
    dsi.id = sp->hdr.id;
    dsi.name = mi.name;
    dsi.type = mi.type;
    dsi.interval = mi.interval;
    dsi.first = first;
    dsi.last = last + mi.interval;
    if (!notify->onDbSeriesStart(dsi)) {
        *count = 1;
        return kAborted;
    }
    for (;;) {
        *count += 1;
        if (!notify->onDbSample(sp->hdr.id, in->time, in->value))
            return kAborted;
        if (!++in)
            return kMore;
        if (in->time > last)
            return kComplete;
    }
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

    // Round time to metric's sampling interval.
    first -= first.time_since_epoch() % mi.interval;
    last -= last.time_since_epoch() % mi.interval;
    // Expand range to include presamples.
    if (presamples) {
        if (presamples * mi.interval < first - TimePoint{}) {
            first -= presamples * mi.interval;
        } else {
            first = {};
        }
    }

    pgno_t lastPage = {};
    if (!findLastSamplePage(txn, &lastPage, id))
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);

    auto sp = txn.pin<SamplePage>(lastPage);
    auto lastSampleTime = sp->lastTime;
    auto firstSampleTime = lastSampleTime - mi.retention + mi.interval;
    if (first < firstSampleTime)
        first = firstSampleTime;
    if (last > lastSampleTime)
        last = lastSampleTime;
    if (first > last)
        return noSamples(notify, id, mi.name, mi.type, last, mi.interval);

    unsigned count = 0;
    GetSampleResult result = {};
    if (sp->sampleIndex == npos) {
        result = reportSamples(&count, notify, txn, mi, sp, first, last);
    } else {
        SampleIndexRec rec = {
            .time = first,
            .pgno = pgno_t::npos,
        };
        auto key = ::trieKey(rec);
        DbSamplePageHeap heap(&txn, this, sp->sampleIndex);
        StrTrieBase trie(&heap);
        auto i = trie.findLessEqual(key);
        if (!i)
            i = trie.begin();
        for (; i; ++i) {
            if (!::parseTrieKey(&rec, *i)) {
                logMsgError() << "Malformed sample page key of '"
                    << mi.name << "'";
                result = kComplete;
                break;
            }
            sp = txn.pin<SamplePage>(rec.pgno);
            result = reportSamples(&count, notify, txn, mi, sp, first, last);
            if (result != kMore)
                break;
        }
    }

    if (result == kAborted) {
        // Send no more updates after an abort.
        return;
    } else if (!count) {
        noSamples(notify, id, mi.name, mi.type, last, mi.interval);
    } else {
        notify->onDbSeriesEnd(id);
    }
}
