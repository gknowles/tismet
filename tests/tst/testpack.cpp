// Copyright Glen Knowles 2023 - 2025.
// Distributed under the Boost Software License, Version 1.0.
//
// testpack.cpp - tismet test
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

#define EXPECT_AT(sloc, ...) \
    if (!bool(__VA_ARGS__)) { \
        logMsgError() << "Line " << sloc.line() << ": EXPECT(" \
            << #__VA_ARGS__ << ") failed"; \
    }

#define EXPECT(...) \
    EXPECT_AT(source_location::current(), __VA_ARGS__)


/****************************************************************************
*
*   Helpers
*
***/

/****************************************************************************
*
*   Test
*
***/

namespace {

class Test : public ITest {
public:
    Test();
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
Test::Test()
    : ITest("pack", "Sample compression tests.")
{}

//===========================================================================
void Test::onTestRun() {
    string buf;
    buf.resize(40);
    DbPack pack(buf.data(), buf.size());
    EXPECT(pack.capacity() == 40);
    EXPECT(pack.size() == 0)
    EXPECT(pack.bits() == 0);
    EXPECT(pack.span().size() == 0);
    DbUnpackIter unpack;

    auto today = chrono::floor<chrono::days>(timeNow());
    auto t1998 = timeFromUnix(900'000'000);
    TimePoint tomorrow = today + 24h;

    struct {
        TimePoint time;
        double value;
        source_location sloc = source_location::current();
    } vals[] = {
        { today + 1s, 1.0 },
        { today + 2s, 2.0 },
        { today + 3s, 3.0 },
        { today + 6s, 3.0 },
        { today + 8s, 5.0 },
        { today + 9s, 7.0 },
    };

    // Insert and read back samples.
    DbPackState stToday = { .sample = { .time = today } };
    pack.retarget(0, stToday);
    for (auto&& val : vals)
        pack.put(val.time, val.value);
    unpack = pack.find(0, stToday);
    for (auto&& val : vals) {
        auto & s = *unpack;
        EXPECT_AT(val.sloc, s.time == val.time);
        EXPECT_AT(val.sloc, s.value == val.value);
        ++unpack;
    }
    EXPECT(!unpack);

    // Insert sample into middle of pack.
    string buf2;
    buf2.resize(40);
    DbPack pack2(buf2.data(), buf2.size());
    struct {
        TimePoint time;
        double value;
        source_location sloc = source_location::current();
    } adds[] = {
        { today + 7s, 4.0 },
    };
    unpack = pack.begin();
    for (auto&& add : adds) {
        for (; unpack && unpack->time < add.time; ++unpack) {
            pack2.put(unpack->time, unpack->value);
        }
        pack2.put(add.time, add.value);
        if (unpack && unpack->time == add.time)
            ++unpack;
    }
    for (; unpack; ++unpack)
        pack2.put(unpack->time, unpack->value);

    unpack = pack2.begin();
    if (s_verbose) {
        for (; unpack; ++unpack)
            cout << unpack->time << ", " << unpack->value << '\n';
    }

    // First sample older than base time.
    DbPackState stTomorrow = { .sample = { .time = tomorrow }};
    pack.retarget(0, stTomorrow);
    pack.put(vals[0].time, vals[0].value);
    unpack = pack.find(0, stTomorrow);
    auto & s = *unpack;
    EXPECT(s.time == vals[0].time);
    EXPECT(s.value == vals[0].value);
    ++unpack;
    EXPECT(!unpack);
}
