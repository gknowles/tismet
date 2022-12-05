// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// testeval.cpp - tismet test
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
        logMsgError() << "Line " << (line ? line : __LINE__) << ": EXPECT(" \
                      << #__VA_ARGS__ << ") failed";                        \
    }


namespace {

struct TestEvalSeries {
    string name;
    TimePoint first;
    Duration interval{};
    vector<double> samples;
};

class UnitTest : public ListBaseLink<>, IEvalNotify {
public:
    UnitTest(string_view name, int line = __LINE__);
    UnitTest(const UnitTest & from);

    UnitTest & query(
        string_view query,
        time_t first,
        unsigned querySeconds,
        unsigned maxPoints = 0
    );
    UnitTest & in(
        string_view name,
        int64_t start,
        Duration interval,
        vector<double> && samples
    );
    UnitTest & out(
        string_view name,
        int64_t start,
        Duration interval,
        vector<double> && samples
    );
    void onTest(DbHandle h);

    const string & name() const { return m_name; }

private:
    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(uint32_t id, TimePoint time, double value) override;
    void onEvalError(string_view errmsg) override;
    void onEvalEnd() override;

    string m_name;
    int m_line{};
    string m_query;
    TimePoint m_first;
    TimePoint m_last;
    unsigned m_maxPoints{};
    vector<TestEvalSeries> m_in;
    vector<TestEvalSeries> m_out;

    vector<TestEvalSeries> m_found;
    string m_errmsg;
    bool m_done{};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static List<UnitTest> s_unitTests;
static mutex s_mut;
static condition_variable s_cv;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool operator==(const TestEvalSeries & a, const TestEvalSeries & b) {
    if (a.samples.size() != b.samples.size())
        return false;
    for (int i = 0; i < a.samples.size(); ++i) {
        if (a.samples[i] != b.samples[i]
            && (!isnan(a.samples[i]) || !isnan(b.samples[i]))
        ) {
            return false;
        }
    }
    return a.name == b.name
        && a.first == b.first
        && a.interval == b.interval;
}

//===========================================================================
static bool operator<(const TestEvalSeries & a, const TestEvalSeries & b) {
    if (int rc = a.name.compare(b.name))
        return rc < 0;
    if (int64_t rc = (a.first - b.first).count())
        return rc < 0;
    if (int64_t rc = (a.interval - b.interval).count())
        return rc < 0;
    return a.samples < b.samples;
}


/****************************************************************************
*
*   UnitTest
*
***/

//===========================================================================
UnitTest::UnitTest(string_view name, int line)
    : m_name{name}
    , m_line{line}
{
    s_unitTests.link(this);
}

//===========================================================================
UnitTest::UnitTest(const UnitTest & from)
    : m_name{from.m_name}
    , m_line{from.m_line}
    , m_query(from.m_query)
    , m_first{from.m_first}
    , m_last{from.m_last}
    , m_maxPoints{from.m_maxPoints}
    , m_in(from.m_in)
    , m_out(from.m_out)
{
    s_unitTests.link(this);
}

//===========================================================================
UnitTest & UnitTest::query(
    string_view query,
    time_t first,
    unsigned querySeconds,
    unsigned maxPoints
) {
    m_query = query;
    m_first = timeFromUnix(first);
    m_last = m_first + (seconds) (querySeconds - 1);
    m_maxPoints = maxPoints;
    return *this;
}

//===========================================================================
UnitTest & UnitTest::in(
    string_view name,
    int64_t start,
    Duration interval,
    vector<double> && samples
) {
    auto & tmp = m_in.emplace_back();
    tmp.name = name;
    tmp.first = timeFromUnix(start);
    tmp.interval = interval;
    tmp.samples = move(samples);
    return *this;
}

//===========================================================================
UnitTest & UnitTest::out(
    string_view name,
    int64_t start,
    Duration interval,
    vector<double> && samples
) {
    auto & tmp = m_out.emplace_back();
    tmp.name = name;
    tmp.first = timeFromUnix(start);
    tmp.interval = interval;
    tmp.samples = move(samples);
    return *this;
}

//===========================================================================
void UnitTest::onTest(DbHandle h) {
    UnsignedSet ids;
    dbFindMetrics(&ids, h);
    for (auto && id : ids)
        dbEraseMetric(h, id);
    for (auto && s : m_in) {
        uint32_t id;
        dbInsertMetric(&id, h, s.name);
        DbMetricInfo info{};
        dbUpdateMetric(h, id, info);
        auto time = s.first;
        for (auto && sample : s.samples) {
            if (!isnan(sample))
                dbUpdateSample(h, id, time, sample);
            time += s.interval;
        }
    }

    m_found.clear();
    m_errmsg.clear();
    m_done = false;
    evaluate(this, m_query, m_first, m_last, m_maxPoints);

    unique_lock lk{s_mut};
    while (!m_done)
        s_cv.wait(lk);

    sort(m_out.begin(), m_out.end(), [](auto & a, auto & b) { return a < b; });
    sort(
        m_found.begin(), m_found.end(),
        [](auto & a, auto & b) { return a < b; }
    );
    bool matched = equal(
        m_out.begin(), m_out.end(),
        m_found.begin(), m_found.end(),
        [](auto & a, auto & b) { return a == b; }
    );
    if (!matched) {
        if (m_errmsg.size())
            logMsgInfo() << m_errmsg;
        logMsgError() << "Query failed, " << m_query;
    }
}

//===========================================================================
bool UnitTest::onDbSeriesStart(const DbSeriesInfo & info) {
    auto & s = m_found.emplace_back();
    s.name = info.name;
    s.first = m_first - m_first.time_since_epoch() % info.interval;
    s.interval = info.interval;
    if (info.interval > 0s) {
        auto count = (m_last - s.first) / info.interval + 1;
        s.samples.resize(count, NAN);
    }
    return true;
}

//===========================================================================
bool UnitTest::onDbSample(uint32_t id, TimePoint time, double value) {
    auto & s = m_found.back();
    auto pos = (time - s.first) / s.interval;
    assert(pos >= 0 && pos < (int) s.samples.size());
    s.samples[pos] = value;
    return true;
}

//===========================================================================
void UnitTest::onEvalError(string_view errmsg) {
    m_errmsg = errmsg;
    onEvalEnd();
}

//===========================================================================
void UnitTest::onEvalEnd() {
    {
        scoped_lock lk{s_mut};
        m_done = true;
    }
    s_cv.notify_one();
}


/****************************************************************************
*
*   Tests
*
***/

//===========================================================================
// consolidate points
//===========================================================================
static auto s_maxpoints_odd = UnitTest("consolidate points")
    .query("*.value", 9, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 8, 2s, {NAN, 1.5, 3.5, 5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 8, 2s, {NAN, NAN, 4, 5});
static auto s_maxpoints = UnitTest("consolidate points even")
    .query("*.value", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {1.5, 3.5, 5.5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 5.5});

//===========================================================================
// aggregate
//===========================================================================
static auto s_aggregate_median = UnitTest("aggregate_median")
    .query("aggregate(*.value, 'median')", 0, 6)
    .in("1.value", 0, 1s, {NAN,NAN,NAN,NAN,1,4})
    .in("2.value", 0, 1s, {NAN,NAN,1,  2,  2,3})
    .in("3.value", 0, 1s, {NAN,NAN,2,  1,  3,2})
    .in("4.value", 0, 1s, {NAN,1,  1,  2,  3,1})
    .out("medianSeries(*.value)", 0, 1s, {NAN, 1, 1, 2, 2.5, 2.5});

//===========================================================================
// consolidateBy
//===========================================================================
static auto s_consolidateBy_average = UnitTest("consolidateBy_average")
    .query("consolidateBy(*.value, 'average')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {1.5, 3.5, 5.5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 5.5});
static auto s_consolidateBy_count = UnitTest("consolidateBy_count")
    .query("consolidateBy(*.value, 'count')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {2, 2, 2})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {0, 1, 2});
static auto s_consolidateBy_diff = UnitTest("consolidateBy_diff")
    .query("consolidateBy(*.value, 'diff')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {-1, -1, -1})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, -1});
static auto s_consolidateBy_first = UnitTest("consolidateBy_first")
    .query("consolidateBy(*.value, 'first')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {1, 3, 5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 5});
static auto s_consolidateBy_last = UnitTest("consolidateBy_last")
    .query("consolidateBy(*.value, 'last')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {2, 4, 6})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 6});
static auto s_consolidateBy_max = UnitTest("consolidateBy_max")
    .query("consolidateBy(*.value, 'max')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {2, 4, 6})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 6});
static auto s_consolidateBy_min = UnitTest("consolidateBy_min")
    .query("consolidateBy(*.value, 'min')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {1, 3, 5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 5});
static auto s_consolidateBy_multiply = UnitTest("consolidateBy_multiply")
    .query("consolidateBy(*.value, 'multiply')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {2, 12, 30})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 30});
static auto s_consolidateBy_range = UnitTest("consolidateBy_range")
    .query("consolidateBy(*.value, 'range')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {1, 1, 1})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 0, 1});
static auto s_consolidateBy_stddev = UnitTest("consolidateBy_stddev")
    .query("consolidateBy(*.value, 'stddev')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {0.5, 0.5, 0.5})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 0, 0.5});
static auto s_consolidateBy_sum = UnitTest("consolidateBy_sum")
    .query("consolidateBy(*.value, 'sum')", 10, 6, 3)
    .in("1.value", 10, 1s, {1,2,3,4,5,6})
    .out("1.value", 10, 2s, {3, 7, 11})
    .in("2.value", 13, 1s, {4,5,6,7,8,9})
    .out("2.value", 10, 2s, {NAN, 4, 11});

//===========================================================================
// countSeries
//===========================================================================
static auto s_countSeries = UnitTest("countSeries")
    .query("countSeries(*.value)", 0, 2)
    .in("1.value", 0, 1s, {1,NAN})
    .in("2.value", 0, 1s, {NAN,2})
    .out("countSeries(*.value)", 0, 1s, {2, 2});
static auto s_countSeries_one = UnitTest("countSeries_one")
    .query("countSeries(*.value)", 0, 2)
    .in("1.value", 0, 1s, {1,NAN})
    .out("countSeries(*.value)", 0, 1s, {1, 1});
static auto s_countSeries_zero = UnitTest("countSeries_zero")
    .query("countSeries(*.value)", 0, 2)
    .out("countSeries(*.value)", 0, 1s, {0, 0});

//===========================================================================
// diffSeries
//===========================================================================
static auto s_diffSeries = UnitTest("diffSeries")
    .query("diffSeries(all.total, alias(*.value, 'values'))", 0, 9)
    .in("all.total", 0, 1s, {10,10,10,10,NAN,NAN,NAN,NAN,10})
    .in("1.value", 0, 1s, {NAN,NAN,2,2,NAN,NAN,2,2,3})
    .in("2.value", 0, 1s, {NAN,2,NAN,2,NAN,2,NAN,2,3})
    .out("diffSeries(*.value)", 0, 1s, {10,8,8,6,NAN,NAN,NAN,NAN,4});

//===========================================================================
// keepLastValue
//===========================================================================
static auto s_keepLastValue = UnitTest("keepLastValue")
    .query("keepLastValue(*.value, 2)", 1, 20)
    .in("1.value", 1, 1s,
        {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20})
    .out("keepLastValue(1.value)", 1, 1s,
        {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20})
    .in("2.value", 1, 1s,
        {NAN,2,NAN,4,NAN,6,NAN,8,NAN,10,NAN,12,NAN,14,NAN,16,NAN,18,NAN,20})
    .out("keepLastValue(2.value)", 1, 1s,
        {NAN,2,2,4,4,6,6,8,8,10,10,12,12,14,14,16,16,18,18,20})
    .in("3.value", 1, 1s,
        {1,2,NAN,NAN,NAN,6,7,8,9,10,11,12,13,14,15,16,17,NAN,NAN,NAN})
    .out("keepLastValue(3.value)", 1, 1s,
        {1,2,NAN,NAN,NAN,6,7,8,9,10,11,12,13,14,15,16,17,NAN,NAN,NAN})
    .in("4.value", 1, 1s,
        {1,2,3,4,NAN,6,NAN,NAN,9,10,11,NAN,13,NAN,NAN,NAN,NAN,18,19,20})
    .out("keepLastValue(4.value)", 1, 1s,
        {1,2,3,4,4,6,6,6,9,10,11,11,13,NAN,NAN,NAN,NAN,18,19,20})
    .in("5.value", 1, 1s,
        {1,2,NAN,NAN,NAN,6,7,8,9,10,11,12,13,14,15,16,17,18,NAN,NAN})
    .out("keepLastValue(5.value)", 1, 1s,
        {1,2,NAN,NAN,NAN,6,7,8,9,10,11,12,13,14,15,16,17,18,18,18})
    .in("6.value", 0, 1s,
        {1,NAN,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3})
    .out("keepLastValue(6.value)", 1, 1s,
        {1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3});

//===========================================================================
// maximumAbove
//===========================================================================
static auto s_maximumAbove = UnitTest("maximumAbove")
    .query("maximumAbove(*.value, 3)", 0, 4)
    .in("1.value", 0, 1s, {0, 1, 2})
    .in("2.value", 0, 1s, {1, 2, 3})
    .in("3.value", 0, 1s, {2, 3, 4})
    .in("4.value", 0, 1s, {4, 5, 6})
    .out("3.value", 0, 1s, {2, 3, 4, NAN})
    .out("4.value", 0, 1s, {4, 5, 6, NAN});

//===========================================================================
// maxSeries
//===========================================================================
static auto s_maxSeries = UnitTest("maxSeries")
    .query("maxSeries(*.value)", 0, 6)
    .in("1.value", 0, 1s, {NAN, 0, 1, 2, 3, NAN})
    .in("2.value", 0, 1s, {0, 1, 2, 3, NAN, NAN})
    .in("3.value", 0, 1s, {1, 2, 3, NAN, 0, NAN})
    .out("maxSeries(*.value)", 0, 1s, {1, 2, 3, 3, 3, NAN});

//===========================================================================
// minSeries
//===========================================================================
static auto s_minSeries = UnitTest("minSeries")
    .query("minSeries(*.value)", 0, 6)
    .in("1.value", 0, 1s, {NAN, 0, 1, 2, 3, NAN})
    .in("2.value", 0, 1s, {0, 1, 2, 3, NAN, NAN})
    .in("3.value", 0, 1s, {1, 2, 3, NAN, 0, NAN})
    .out("minSeries(*.value)", 0, 1s, {0, 0, 1, 2, 0, NAN});

//===========================================================================
// movingAverage
//===========================================================================
static auto s_movingAverage = UnitTest("movingAverage")
    .query("movingAverage(*.value, 4)", 100, 4)
    .in("1.value", 0, 1s, {0})
    .out("movingAverage(1.value)", 100, 1s, {NAN,NAN,NAN,NAN})
    .in("2.value", 100, 1s, {NAN, 0, 1, 2})
    .out("movingAverage(2.value)", 100, 1s, {NAN, 0, 0.25, 0.75 })
    .in("3.value", 96, 1s, {0, 1, 2, 3, 4, 5, 6, 7})
    .out("movingAverage(3.value)", 100, 1s, {2.5, 3.5, 4.5, 5.5});
static auto s_movingAverage_time = UnitTest("movingAverage_time")
    .query("movingAverage(*.value, '210s')", 1000, 240)
    .in("1.value", 760, 60s, {0, 1, 2, 3, 4, 5, 6, 7})
    .out("movingAverage(1.value)", 960, 60s, {2.5, 3.5, 4.5, 5.5, 4.5});

//===========================================================================
// nonNegativeDerivative
//===========================================================================
static auto s_nonNegativeDerivative = UnitTest("nonNegativeDerivative")
    .query("nonNegativeDerivative(*.value)", 1, 10)
    .in("1.value", 1, 1s, {NAN,1,2,3,4,5,NAN,3,2,1})
    .out("nonNegativeDerivative(1.value)", 1, 1s,
        {NAN,NAN,1,1,1,1,NAN,NAN,NAN,NAN})
    .in("2.value", 0, 1s, {1, 2, 3})
    .out("nonNegativeDerivative(2.value)", 1, 1s,
        {1,1,NAN,NAN,NAN,NAN,NAN,NAN,NAN,NAN});
static auto s_nonNegativeDerivative_max = UnitTest("nonNegativeDerivative_max")
    .query("nonNegativeDerivative(1.value, 5)", 1, 10)
    .in("1.value", 1, 1s, {0,1,2,3,4,5,0,1,2,3})
    .out("nonNegativeDerivative(1.value)", 1, 1s,
        {NAN,1,1,1,1,1,1,1,1,1});

//===========================================================================
// scaleToSeconds
//===========================================================================
static auto s_scaleToSeconds = UnitTest("scaleToSeconds")
    .query("scaleToSeconds(*.value, 30)", 0, 600)
    .in("1.value", 0, 60s, {1,2,3,4,5,6,7,8,9,10})
    .out("scaleToSeconds(1.value)",0,60s, {0.5,1,1.5,2,2.5,3,3.5,4,4.5,5})
    .in("2.value", 0, 60s, {NAN,2,NAN,4,NAN,6,NAN,8,NAN,10})
    .out("scaleToSeconds(2.value)",0,60s, {NAN,1,NAN,2,NAN,3,NAN,4,NAN,5})
    .in("3.value", 0, 60s, {1,2,NAN,NAN,NAN,6,7,8,9,10})
    .out("scaleToSeconds(3.value)",0,60s, {0.5,1,NAN,NAN,NAN,3,3.5,4,4.5,5})
    .in("4.value", 0, 60s, {1,2,3,4,5,6,7,8,9,NAN})
    .out("scaleToSeconds(4.value)",0,60s, {0.5,1,1.5,2,2.5,3,3.5,4,4.5,NAN});

//===========================================================================
// stddevSeries
//===========================================================================
static auto s_stddevSeries = UnitTest("stddevSeries")
    .query("stddevSeries(*.value)", 0, 1)
    .in("1.value", 0, 1s, {1})
    .in("2.value", 0, 1s, {2})
    .in("3.value", 0, 1s, {3})
    .in("4.value", 0, 1s, {4})
    .out("stddevSeries(*.value)", 0, 1s, {sqrt(5.0 / 4.0)});

//===========================================================================
// timeShift
//===========================================================================
static auto s_timeShift = UnitTest("timeShift")
    .query("timeShift(*.value, '2s')", 100, 5)
    .in("1.value", 95, 1s, {-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8,9})
    .out("timeShift(1.value)", 100, 1s,
        {-2,-1,0,1,2})
    .in("2.value", 103, 1s, {0,1,2,3,4})
    .out("timeShift(2.value)", 100, 1s,
        {NAN,NAN,NAN,NAN,NAN});


/****************************************************************************
*
*   Public API
*
***/

namespace {

class Test : public ITest {
public:
    Test() : ITest("eval", "Function evaluation tests.") {}
    void onTestDefine(Cli & cli) override;
    void onTestRun() override;

private:
    vector<string> m_subtests;
    bool m_verbose{false};
};

} // namespace

static Test s_test;

//===========================================================================
void Test::onTestDefine(Cli & cli) {
    auto & subs = cli.optVec(&m_subtests, "[subtests]")
        .desc("Specific function tests to run, defaults to all.");
    for (auto && ut : s_unitTests) {
        subs.choice(ut.name(), ut.name());
    }
    cli.opt(&m_verbose, "v verbose")
        .desc("Display test progress.");
}

//===========================================================================
void Test::onTestRun() {
    int line = 0;

    auto start = timeFromUnix(900'000'000);
    auto name = "this.is.metric.1"s;

    const char dat[] = "test";
    auto h = dbOpen(dat, 128, fDbOpenCreat | fDbOpenTrunc);
    EXPECT(h && "Failure to create database");
    if (!h)
        return;

    evalInitialize(h);
    unordered_map<string, bool> tests;
    for (auto && sub : m_subtests)
        tests[sub] = false;
    for (auto && ut : s_unitTests) {
        if (!tests.empty()) {
            if (!tests.count(ut.name()))
                continue;
            tests[ut.name()] = true;
        }
        if (m_verbose)
            cout << ut.name() << "...\n";
        ut.onTest(h);
    }
    for (auto && [name, found] : tests) {
        if (!found)
            logMsgError() << "Unknown test, " << name;
    }

    dbClose(h);
}
