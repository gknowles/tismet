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

#define EXPECT(...) \
    if (!bool(__VA_ARGS__)) { \
        logMsgError() << "Line " << __LINE__ << ": EXPECT(" \
            << #__VA_ARGS__ << ") failed"; \
    }


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
    EXPECT(pack.unusedBits() == 0);
    EXPECT(pack.view().size() == 0);

    auto today = std::chrono::floor<std::chrono::days>(timeNow());

    struct {
        TimePoint time;
        double value;
    } vals[] = {
        { today + 1s, 1.0 },
        { today + 2s, 2.0 },
        { today + 3s, 3.0 },
        { today + 6s, 3.0 },
        { today + 8s, 5.0 },
        { today + 9s, 7.0 },
    };
    for (auto&& [t, v] : vals)
        pack.put(t, v);
    DbUnpackIter unpack(pack.data(), pack.size(), pack.unusedBits());
    for (auto&& [t, v] : vals) {
        auto & s = *unpack;
        EXPECT(s.time == t);
        EXPECT(s.value == v);
        ++unpack;
    }
    EXPECT(!unpack);

    string buf2;
    buf2.resize(40);
    DbPack pack2(buf2.data(), buf2.size());
    struct {
        TimePoint time;
        double value;
    } adds[] = {
        { today + 7s, 4.0 },
    };
    unpack = pack;
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

    unpack = pack2;
    if (s_verbose) {
        for (; unpack; ++unpack)
            cout << unpack->time << ", " << unpack->value << '\n';
    }
}
