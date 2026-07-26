// Copyright Glen Knowles 2017 - 2025.
// Distributed under the Boost Software License, Version 1.0.
//
// testdb.cpp - tismet test
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

#define EXPECT_AT(sloc, ...)                                                \
    if (!bool(__VA_ARGS__)) {                                               \
        logMsgError() << "Line " << sloc.line() << ": EXPECT("              \
            << #__VA_ARGS__ << ") failed";                                  \
    }

#define EXPECT(...) \
    EXPECT_AT(source_location::current(), __VA_ARGS__)

namespace {

struct UpdateInfo {
    Duration sinceStart;
    double value;
    bool reopen = false;
    source_location sloc = source_location::current();
};

} // namespace


/****************************************************************************
*
*   TestDbSeries
*
***/

namespace {

struct TestDbSeries : IDbDataNotify {
    string m_name;
    uint32_t m_id{};
    TimePoint m_first;
    Duration m_interval;
    unsigned m_count{};
    vector<pair<TimePoint, double>> m_samples;

    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(
        uint32_t id,
        Dim::TimePoint time,
        double value
    ) override;
};

} // namespace

//===========================================================================
bool TestDbSeries::onDbSeriesStart(const DbSeriesInfo & info) {
    m_name = info.name;
    m_id = info.id;
    m_first = info.first;
    m_interval = info.interval;
    m_count = 0;
    if (empty(m_interval)) {
        m_samples.clear();
    } else {
        auto count = (info.last - info.first) / info.interval;
        m_samples.reserve(count);
        m_samples.resize(0);
    }
    return true;
}

//===========================================================================
bool TestDbSeries::onDbSample(
    uint32_t id,
    Dim::TimePoint time,
    double value
) {
    m_samples.push_back({time, value});
    m_count += 1;
    return true;
}


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool compareSamples(
    TestDbSeries * samples,
    DbHandle h,
    uint32_t id,
    const map<TimePoint, double> & expected
) {
    dbGetSamples(samples, h, id);
    auto ix = expected.begin();
    auto i = 0;
    for (auto&& samp : samples->m_samples) {
        if (ix == expected.end())
            return false;
        if (samp.first != ix->first || samp.second != ix->second)
            return false;
        ++ix;
        ++i;
    }
    return ix == expected.end();
}

//===========================================================================
static DbStats reopen(
    DbHandle * ph,
    DbContext * ctx,
    uint32_t id,
    string_view name,
    auto sloc = source_location::current()
) {
    auto h = *ph;
    auto info = dbQueryInfo(h);
    ctx->reset();
    auto stats = dbQueryStats(h);
    dbClose(h);
    h = dbOpen(
        info.datafile,
        info.flags & ~(fDbOpenNew | fDbOpenAlways | fDbOpenTrunc)
    );
    if (!h) {
        EXPECT_AT(sloc, h && "Failure to reopen database");
    } else {
        info = dbQueryInfo(h);
        stats = dbQueryStats(h);
        ctx->reset(h);
        auto count = dbInsertMetric(&id, h, name);
        EXPECT_AT(sloc, "metrics inserted" && count == 0);
    }
    *ph = h;
    return stats;
}

//===========================================================================
static void addSamples(
    DbHandle * ph,
    DbContext * ctx,
    uint32_t id,
    TimePoint start,
    string_view name,
    const vector<UpdateInfo> & vals
) {
    DbHandle h = *ph;
    assert(h == ctx->handle());
    TestDbSeries samples;
    auto info = dbQueryInfo(h);
    auto stats = dbQueryStats(h);
    if (!dbGetSamples(&samples, h, id)) {
        EXPECT_AT(vals.front().sloc, !"Unable to preload samples");
        return;
    }
    map<TimePoint, double> expected;
    for (auto&& samp : samples.m_samples) {
        expected[samp.first] = samp.second;
    }
    for (auto&& val : vals) {
        auto time = start + val.sinceStart;
        auto value = val.value;
        dbUpdateSample(h, id, time, value);
        expected[time] = value;
        if (val.reopen)
            stats = reopen(&h, ctx, id, name, val.sloc);
        if (!compareSamples(&samples, h, id, expected))
            EXPECT_AT(val.sloc, !"Expected samples don't match.");
    }
    *ph = h;
}

//===========================================================================
static void setFullSamplePage(
    uint32_t * id,
    map<TimePoint, double> * out,
    DbHandle h,
    string name,
    TimePoint start
) {
    dbEraseMetric(h, *id);
    out->clear();
    if (!dbInsertMetric(id, h, name)) {
        EXPECT(!"Failure replacing metric.");
        return;
    }
    DbMetricInfo info;
    info.type = kSampleTypeFloat32;
    info.interval = 1min;
    info.retention = duration_cast<Duration>(300 * info.interval);
    dbUpdateMetric(h, *id, info);
    dbUpdateSample(h, *id, start, 0);
    (*out)[start] = 0;
    auto stats = dbQueryStats(h);
    auto oldFree = stats.freePages;
    while (oldFree == stats.freePages) {
        auto pos = out->size();
        auto value = 1.0 * pos + (pos % 2 ? 0.5 : 0.0);
        auto time = start + pos * 1min;
        dbUpdateSample(h, *id, time, value);
        (*out)[time] = value;
        stats = dbQueryStats(h);
    }
    TestDbSeries samples;
    if (!compareSamples(&samples, h, *id, *out))
        EXPECT(!"Fill first page: expected samples don't match.");
}


/****************************************************************************
*
*   Test
*
***/

namespace {

class Test : public ITest {
public:
    Test();
    void invalidFileTests();
    void dataTests();
    void queryTests();
    void sampleTests();
    void readonlyTests();

    // Inherited via ITest
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
Test::Test()
    : ITest("db", "Database manipulation tests.")
{}

//===========================================================================
void Test::invalidFileTests() {
    auto invalidPrefix = Path("dir");
    auto invalidWal = Path(invalidPrefix).setExt(".tsl");
    auto invalidData = Path(invalidPrefix).setExt(".tsd");
    auto invalidWork = Path(invalidPrefix).setExt(".tsw");
    fileRemove(invalidWal, true);
    fileRemove(invalidData, true);
    fileRemove(invalidWork, true);

    fileCreateDirs(invalidWal);
    testLogMsgs({{
        kLogTypeError,
        "Open failed (system:5), " + invalidWal.str()
    }});
    auto h = dbOpen(invalidWal, fDbOpenAlways | fDbOpenTrunc);
    EXPECT(!h && "Open of directory as file should have failed.");
    bool found = false;
    EXPECT(!fileDirExists(&found, invalidWal) && found);
    EXPECT(!fileExists(&found, invalidData) && !found);
    EXPECT(!fileExists(&found, invalidWork) && !found);
    fileRemove(invalidWal);

    fileCreateDirs(invalidData);
    testLogMsgs({{kLogTypeError, "Open failed, " + invalidData.str()}});
    h = dbOpen(invalidData, fDbOpenAlways | fDbOpenTrunc);
    EXPECT(!h && "Open of directory as file should have failed.");
    EXPECT(!fileExists(&found, invalidWal) && !found);
    EXPECT(!fileDirExists(&found, invalidData) && found);
    EXPECT(!fileExists(&found, invalidWork) && !found);
    fileRemove(invalidData);

    fileCreateDirs(invalidWork);
    testLogMsgs({{kLogTypeError, "Open failed, " + invalidWork.str()}});
    h = dbOpen(invalidWork, fDbOpenAlways | fDbOpenTrunc);
    EXPECT(!h && "Open of directory as file should have failed.");
    EXPECT(!fileExists(&found, invalidWal) && !found);
    EXPECT(!fileExists(&found, invalidData) && !found);
    EXPECT(!fileDirExists(&found, invalidWork) && found);
    fileRemove(invalidWork);
}

//===========================================================================
void Test::dataTests() {
    auto start = timeFromUnix(900'000'000);
    auto name = "this.is.metric.1"s;

    const char dat[] = "test";
    auto h = dbOpen(dat, fDbOpenAlways | fDbOpenTrunc, 128);
    EXPECT(h && "Failure to create database");
    if (!h)
        return;

    auto stats = dbQueryStats(h);
    EXPECT(stats.metrics == 0);
    EXPECT(stats.pageSize == 128);
    //EXPECT(stats.numPages == 4);
    //EXPECT(stats.freePages == 0);
    auto spp = 100u;
    auto pgt = spp * 1min;
    DbContext ctx(h);
    uint32_t id;
    unsigned count = 0;
    count = dbInsertMetric(&id, h, name);
    EXPECT("metrics inserted" && count == 1);
    stats = dbQueryStats(h);
    DbMetricInfo info;
    info.type = kSampleTypeFloat32;
    info.retention = duration_cast<Duration>(6.5 * pgt);
    info.interval = 1min;
    dbUpdateMetric(h, id, info);

    // erase metric
    dbEraseMetric(h, id);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 13);
    //EXPECT(stats.freePages == 6);
    EXPECT(stats.metrics == 0);

    count = 0;
    for (int i = 1; i < 30; ++i) {
        name = "this.is.metric.";
        name += toString(i);
        uint32_t i2;
        count += dbInsertMetric(&i2, h, name);
        dbUpdateSample(h, i2, start, (float) i);
    }
    EXPECT("metrics inserted" && count == 29);
    stats = dbQueryStats(h);
    //EXPECT(stats.freePages == 0);

    UnsignedSet found;
    dbFindMetrics(&found, h, "*.is.*.*5");
    ostringstream os;
    os << found;
    EXPECT(os.str() == "5 15 25");

    for (int i = 100; ; ++i) {
        stats = dbQueryStats(h);
        if (stats.numPages > stats.bitsPerPage)
            break;
        name = "this.is.metric.";
        name += toString(i);
        uint32_t id;
        count += dbInsertMetric(&id, h, name);
        dbUpdateSample(h, id, start, (float) i);
    };
    ctx.reset();
    dbClose(h);

    h = dbOpen(dat);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    dbFindMetrics(&found, h);
    if (found.empty()) {
        EXPECT(!"No metrics after reopen");
    } else {
        id = found.pop_front();
        dbEraseMetric(h, id);
        dbInsertMetric(&id, h, "replacement.metric.1");
    }
    ctx.reset();
    dbClose(h);
}

//===========================================================================
void Test::queryTests() {
    auto start = timeFromUnix(900'000'000);
    const char dat[] = "test";
    UnsignedSet found;
    DbContext ctx;
    DbMetricInfo info;

    auto h = dbOpen(dat);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    auto stats = dbQueryStats(h);
    auto spp = 100u;
    auto pgt = spp * 1min;
    dbFindMetrics(&found, h);
    for (auto && id : found)
        dbEraseMetric(h, id);
    for (auto&& name : { "1.value", "2.value" }) {
        uint32_t id;
        dbInsertMetric(&id, h, name);
    }
    dbFindMetrics(&found, h);
    for (auto && id : found)
        dbEraseMetric(h, id);
    ctx.reset();
    dbClose(h);
}

//===========================================================================
void Test::sampleTests() {
    auto start = timeFromUnix(900'000'000);
    auto name = "this.is.metric.1"s;
    const char dat[] = "test";
    UnsignedSet found;
    DbContext ctx;
    uint32_t id;
    DbMetricInfo info;
    TestDbSeries samples;
    map<TimePoint, double> expected;

    auto h = dbOpen(dat, fDbOpenAlways | fDbOpenTrunc, 128);
    EXPECT(h && "Failure to truncate database");
    if (!h)
        return;
    ctx.reset(h);
    auto stats = dbQueryStats(h);
    auto spp = 100u;
    auto pgt = spp * 1min;
    dbFindMetrics(&found, h);
    for (auto && id : found)
        dbEraseMetric(h, id);
    stats = dbQueryStats(h);
    EXPECT(stats.metrics == 0);

    dbInsertMetric(&id, h, name);
    vector<UpdateInfo> vals = {
        { 0s, 1, true },
        { 0s, 3 },
        { 1min, 4 },
        { -1min, 2 },
        { pgt - 1min, 5 },
        { pgt, 6, true },
        { 2*pgt - 2min, 7 },
        { 4*pgt + 10min, 8 },
        { -2min, 1 },
        { 6*pgt, 6 },
        { 20*pgt, 1 },
    };
    addSamples(&h, &ctx, id, start, name, vals);


    // Page split when appending to end.
    setFullSamplePage(&id, &expected, h, name, start);
    stats = dbQueryStats(h);
    [[maybe_unused]] auto oldFree = stats.freePages;

    // completely fill sample pages
    auto base = 1.0;
    for (auto i = 0u; i < 3 * spp; ++i) {
        auto time = start + i * 1min;
        auto value = base + (i % 2 ? 0.5 : 0.0);
        expected[time] = value;
        dbUpdateSample(h, id, time, value);
        if (!compareSamples(&samples, h, id, expected))
            EXPECT(!"Completely fill (step): expected samples don't match.");
    }
    stats = dbQueryStats(h);
    if (!compareSamples(&samples, h, id, expected))
        EXPECT(!"Completely fill: expected samples don't match.");

    // change all historical sample values
    base = 2.0;
    for (auto i = 0u; i < 3 * spp; ++i) {
        auto time = start + i * 1min;
        auto value = base + (i % 2 ? 0.5 : 0.0);
        expected[time] = value;
        dbUpdateSample(h, id, time, value);
        if (!compareSamples(&samples, h, id, expected))
            EXPECT(!"Change all (step): expected samples don't match.");
    }
    stats = dbQueryStats(h);
    if (!compareSamples(&samples, h, id, expected))
        EXPECT(!"Change all: expected samples don't match.");

    // age out all sample values
    base = 3.0;
    for (auto i = 3 * spp; i < 6 * spp; ++i) {
        auto time = start + i * 1min;
        auto value = base + (i % 2 ? 0.5 : 0.0);
        expected[time] = value;
        dbUpdateSample(h, id, time, value);
    }
    stats = dbQueryStats(h);
    dbGetSamples(&samples, h, id);

    dbGetSamples(
        &samples,
        h,
        id,
        start + (3 * spp - 1) * 1min,
        start + (3 * spp + 2) * 1min
    );
    EXPECT(samples.m_count == 3);

    ctx.reset();
    dbClose(h);
}

//===========================================================================
void Test::readonlyTests() {
    auto start = timeFromUnix(900'000'000);
    const char dat[] = "test";
    UnsignedSet found;
    DbContext ctx;
    DbMetricInfo info;

    auto h = dbOpen(dat, fDbOpenReadOnly);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    auto stats = dbQueryStats(h);
    auto spp = 100u;
    auto pgt = spp * 1min;
    dbFindMetrics(&found, h);
    ctx.reset();
    dbClose(h);
}

//===========================================================================
void Test::onTestRun() {
    invalidFileTests();
    dataTests();
    queryTests();
    sampleTests();
    readonlyTests();
}
