// Copyright Glen Knowles 2017 - 2023.
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

#define EXPECT(...)                                                         \
    if (!bool(__VA_ARGS__)) {                                               \
        logMsgError() << "Line " << source_location::current().line()       \
            << ": EXPECT(" << #__VA_ARGS__ << ") failed";                   \
    }


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
    vector<double> m_samples;

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
    if (!m_interval.count()) {
        m_samples.clear();
    } else {
        auto count = (info.last - info.first) / info.interval;
        m_samples.resize(count, NAN);
    }
    return true;
}

//===========================================================================
bool TestDbSeries::onDbSample(
    uint32_t id,
    Dim::TimePoint time,
    double value
) {
    auto pos = (time - m_first) / m_interval;
    assert(pos >= 0 && pos < (int) m_samples.size());
    m_samples[pos] = value;
    if (!isnan(value))
        m_count += 1;
    return true;
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

private:
    bool m_verbose{false};
};

} // namespace

static Test s_test;

//===========================================================================
Test::Test()
    : ITest("db", "Database manipulation tests.")
{
    m_cli.opt<bool>(&m_verbose, "v verbose")
        .desc("Display additional information during test");
}

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
    auto h = dbOpen(invalidWal, fDbOpenCreat | fDbOpenTrunc);
    EXPECT(!h && "Open of directory as file should have failed.");
    bool found = false;
    EXPECT(!fileDirExists(&found, invalidWal) && found);
    EXPECT(!fileExists(&found, invalidData) && !found);
    EXPECT(!fileExists(&found, invalidWork) && !found);
    fileRemove(invalidWal);

    fileCreateDirs(invalidData);
    testLogMsgs({{kLogTypeError, "Open failed, " + invalidData.str()}});
    h = dbOpen(invalidData, fDbOpenCreat | fDbOpenTrunc);
    EXPECT(!h && "Open of directory as file should have failed.");
    EXPECT(!fileExists(&found, invalidWal) && !found);
    EXPECT(!fileDirExists(&found, invalidData) && found);
    EXPECT(!fileExists(&found, invalidWork) && !found);
    fileRemove(invalidData);

    fileCreateDirs(invalidWork);
    testLogMsgs({{kLogTypeError, "Open failed, " + invalidWork.str()}});
    h = dbOpen(invalidWork, fDbOpenCreat | fDbOpenTrunc);
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
    auto h = dbOpen(dat, fDbOpenCreat | fDbOpenTrunc, 128);
    EXPECT(h && "Failure to create database");
    if (!h)
        return;

    auto stats = dbQueryStats(h);
    EXPECT(stats.metrics == 0);
    EXPECT(stats.pageSize == 128);
    //EXPECT(stats.numPages == 4);
    //EXPECT(stats.freePages == 0);
    auto spp = stats.samplesPerPage[kSampleTypeFloat32];
    auto pgt = spp * 1min;

    DbContext ctx(h);
    uint32_t id;
    unsigned count = 0;
    count += dbInsertMetric(&id, h, name);
    EXPECT("metrics inserted" && count == 1);
    DbMetricInfo info;
    info.type = kSampleTypeFloat32;
    info.retention = duration_cast<Duration>(6.5 * pgt);
    info.interval = 1min;
    dbUpdateMetric(h, id, info);
    dbUpdateSample(h, id, start, 1.0);
    ctx.reset();
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 7);
    dbClose(h);
    EXPECT(count == 1);

    h = dbOpen(dat);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    count = dbInsertMetric(&id, h, name);
    EXPECT("metrics inserted" && count == 0);
    dbUpdateSample(h, id, start, 3.0);
    dbUpdateSample(h, id, start + 1min, 4.0);
    dbUpdateSample(h, id, start - 1min, 2.0);
    // add to first of new page 2
    dbUpdateSample(h, id, start + pgt - 1min, 5.0);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 8);
    // another on page 2
    dbUpdateSample(h, id, start + pgt, 6.0);
    ctx.reset();
    dbClose(h);

    h = dbOpen(dat);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    count = dbInsertMetric(&id, h, name);
    EXPECT("metrics inserted" && count == 0);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 8);
    // add to very end of page 2
    dbUpdateSample(h, id, start + 2 * pgt - 2min, 7.0);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 8);
    // add to new page 5. leaves sample pages 3, 4 unallocated
    dbUpdateSample(h, id, start + 4 * pgt + 10min, 8.0);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 10);
    // add to new historical page, and adds a radix page
    dbUpdateSample(h, id, start - 2min, 1);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 11);
    // circle back onto that historical page, reassigning it's time
    dbUpdateSample(h, id, start + 6 * pgt, 6);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 11);
    EXPECT(stats.freePages == 0);
    EXPECT(stats.metrics == 1);
    // add sample more than the retention period in the future
    dbUpdateSample(h, id, start + 20 * pgt, 1);
    stats = dbQueryStats(h);
    EXPECT(stats.freePages == 5);
    EXPECT(stats.metrics == 1);
    // erase metric
    dbEraseMetric(h, id);
    stats = dbQueryStats(h);
    //EXPECT(stats.numPages == 13);
    //EXPECT(stats.freePages == 6);
    EXPECT(stats.metrics == 0);

    count = 0;
    for (int i = 1; i < 30; ++i) {
        name = "this.is.metric.";
        name += to_string(i);
        uint32_t i2;
        count += dbInsertMetric(&i2, h, name);
        dbUpdateSample(h, i2, start, (float) i);
    }
    EXPECT("metrics inserted" && count == 29);
    stats = dbQueryStats(h);
    EXPECT(stats.freePages == 0);

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
        name += to_string(i);
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
    id = found.pop_front();
    dbEraseMetric(h, id);
    dbInsertMetric(&id, h, "replacement.metric.1");
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
    auto spp = stats.samplesPerPage[kSampleTypeFloat32];
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
    const char dat[] = "test";
    UnsignedSet found;
    DbContext ctx;
    uint32_t id;
    DbMetricInfo info;

    auto h = dbOpen(dat);
    EXPECT(h && "Failure to reopen database");
    if (!h)
        return;
    ctx.reset(h);
    auto stats = dbQueryStats(h);
    auto spp = stats.samplesPerPage[kSampleTypeFloat32];
    auto pgt = spp * 1min;
    dbFindMetrics(&found, h);
    for (auto && id : found)
        dbEraseMetric(h, id);
    stats = dbQueryStats(h);
    dbInsertMetric(&id, h, "this.is.metric.1");
    EXPECT(id == 1);
    dbUpdateSample(h, id, start, 1.0);
    info.type = kSampleTypeFloat32;
    info.retention = duration_cast<Duration>(3 * pgt);
    info.interval = 1min;
    dbUpdateMetric(h, id, info);
    auto pageStart = start;
    auto oldFree = dbQueryStats(h).freePages - 1;
    for (;;) {
        dbUpdateSample(h, id, pageStart, 1.0);
        stats = dbQueryStats(h);
        if (oldFree != stats.freePages)
            break;
        pageStart += 1min;
    }
    oldFree = stats.freePages;
    // fill with homogeneous values to trigger conversion to virtual page
    for (auto time = pageStart; time < pageStart + pgt; time += 1min) {
        dbUpdateSample(h, id, time, 1.0);
    }
    stats = dbQueryStats(h);
    EXPECT(oldFree == stats.freePages - 1);

    // completely fill sample pages
    for (auto i = 0u; i < 3 * spp; ++i) {
        dbUpdateSample(h, id, start + i * 1min, 1.0);
    }
    stats = dbQueryStats(h);

    // change all historical sample values
    for (auto i = 0u; i < 3 * spp; ++i) {
        dbUpdateSample(h, id, start + i * 1min, 2.0);
    }
    stats = dbQueryStats(h);

    // age out all sample values
    for (auto i = 3 * spp; i < 6 * spp; ++i) {
        dbUpdateSample(h, id, start + i * 1min, 3.0);
    }
    stats = dbQueryStats(h);

    TestDbSeries samples;
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
    auto spp = stats.samplesPerPage[kSampleTypeFloat32];
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
