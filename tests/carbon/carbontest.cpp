// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// carbontest.cpp - tismet test carbon
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
namespace fs = std::experimental::filesystem;


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

//===========================================================================
static void internalTest() {
    TimePoint start = Clock::from_time_t(900'000'000);

    EXPECT_PARSE("metric 0.8 900000000\n", 0.8f, start);
    EXPECT_PARSE("metric -0.8e-2 900000000\n", -0.008f, start);
    EXPECT_PARSE("metric 0.8e+2 900000000\n", 80, start);
    EXPECT_PARSE("metric -8 900000000\n", -8, start);
    EXPECT_PARSE("metric 8e+2 900000000\n", 800, start);
}


/****************************************************************************
*     
*   Application
*     
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    auto & test = cli.opt<bool>("test", true).desc("Run internal unit tests");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();
    if (*test)
        internalTest();

    if (int errors = logGetMsgCount(kLogTypeError)) {
        ConsoleScopedAttr attr(kConsoleError);
        cerr << "*** " << errors << " FAILURES" << endl;
        appSignalShutdown(EX_SOFTWARE);
    } else {
        cout << "All tests passed" << endl;
        appSignalShutdown(EX_OK);
    }
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
        | _CRTDBG_DELAY_FREE_MEM_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    int code = appRun(app, argc, argv);
    return code;
}
