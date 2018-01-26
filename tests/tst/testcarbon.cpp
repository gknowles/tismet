// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// testcarbon.cpp - tismet test
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
        logMsgError() << "Line " << (line ? line : __LINE__) << ": EXPECT(" \
            << #__VA_ARGS__ << ") failed"; \
    }
#define EXPECT_PARSE(text, value, time) \
    parseTest(__LINE__, text, value, time)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void parseTest(
    int line,
    string_view text,
    float value,
    TimePoint time,
    string_view name = "metric"
) {
    CarbonUpdate upd;
    bool result = carbonParse(upd, text);
    EXPECT(result);
    EXPECT(upd.name == name);
    EXPECT(upd.value == value);
    EXPECT(upd.time == time);
}


/****************************************************************************
*
*   Test
*
***/

namespace {

class Test : public ITest {
public:
    Test() : ITest("carbon", "Carbon message parsing tests.") {}
    void onTestRun() override;
};

} // namespace

static Test s_test;

//===========================================================================
void Test::onTestRun() {
    TimePoint start = Clock::from_time_t(900'000'000);

    EXPECT_PARSE("metric 0.8 900000000\n", 0.8f, start);
    EXPECT_PARSE("metric -0.8e-2 900000000\n", -0.008f, start);
    EXPECT_PARSE("metric 0.8e+2 900000000\n", 80, start);
    EXPECT_PARSE("metric -8 900000000\n", -8, start);
    EXPECT_PARSE("metric 8e+2 900000000\n", 800, start);
}
