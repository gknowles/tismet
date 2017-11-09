// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// radixtest.cpp - tismet test radix
#include "pch.h"
#pragma hdrstop

using namespace std;
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


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void tests() {
    int line = 0;
    int digits[10];

    DbRadix rd{100, 0, 0, 4095};
    auto count = rd.convert(digits, size(digits), 4032);
    EXPECT(count == 3);
    EXPECT(vector<int>(digits, digits + 3) == vector<int>({6, 11, 7}));

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
*   Application
*
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    auto & pageSize = cli.opt<size_t>("b", 4096)
        .desc("size of pages used by radix index");
    auto & maxPages = cli.opt<size_t>(
        "m", 
        (size_t) numeric_limits<uint32_t>::max() + 1)
        .desc("maximum number of pages allowed in index");
    auto & poff = cli.opt<size_t>("p").desc("offset to list in normal pages");
    auto & roff = cli.opt<size_t>("r").desc("offset to list in root pages")
        .after([&](auto & cli, auto & opt, auto & val) { 
            if (!opt)
                *opt = *pageSize / 2;
            return true;
        });
    auto & vals = cli.optVec<uint32_t>("[value]")
        .desc("values to translate");
    auto & test = cli.opt<bool>("test")
        .desc("run internal unit tests");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();

    if (*test)
        return tests();

    DbRadix rd{*pageSize, *roff, *poff, *maxPages - 1};
    cout << rd << endl;

    int digits[10];
    for (auto && val : *vals) {
        auto num = rd.convert(digits, size(digits), val);
        cout << val << ":";
        for (auto i = 0; i < num; ++i) {
            cout << ' ' << digits[i];
        }
        cout << endl;
    }
    appSignalShutdown(EX_OK);
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
int main(int argc, char * argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
        | _CRTDBG_DELAY_FREE_MEM_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);
    return appRun(app, argc, argv);
}
