// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// querytest.cpp - tismet test query
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
#define EXPECT_PARSE(text, normal) \
    parseTest(__LINE__, text, normal)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void parseTest(
    int line,
    const string & src,
    string_view normal
) {
    QueryInfo qry;
    bool result = queryParse(qry, src);
    EXPECT(result);
    EXPECT(qry.text == normal);
}

//===========================================================================
static void internalTest() {
    TimePoint start = Clock::from_time_t(900'000'000);

    EXPECT_PARSE("a[b]c[de]f", "abc[de]f");
    EXPECT_PARSE("a.{ xxx ,zzz,xxx, yyyyy }.b", "a.{xxx,yyyyy,zzz}.b");
    EXPECT_PARSE("a[62-41]", "a[12346]");
    EXPECT_PARSE("a.b.c", "a.b.c");
    EXPECT_PARSE("sum( a )", "sum(a)");
    EXPECT_PARSE("sum(maximumAbove(a.b[12-46], 2))", 
        "sum(maximumAbove(a.b[12346], 2))");
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
