// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// indextest.cpp - tismet test index
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
#define EXPECT_FIND(query, result) \
    findTest(__LINE__, index, query, result)


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void findTest(
    int line,
    const DbIndex & index,
    string_view query,
    string_view result
) {

    UnsignedSet out;
    index.find(out, query);
    ostringstream os;
    os << out;
    auto found = os.str();
    if (found != result) {
        logMsgError() << "Line " << line << ": EXPECT("
            << found << " == " << result << ") failed";
    }
}

//===========================================================================
static void internalTest() {
    int line = 0;

    DbIndex index;
    index.insert(1, "a.z");
    index.insert(2, "a.b.m.z");
    index.insert(3, "a.m.y.z");
    index.insert(4, "a.b.m.y.z");
    EXPECT(index.size() == 4);

    UnsignedSet ids;
    uint32_t id;
    index.find(id, "a.m.y.z");
    EXPECT(id == 3);

    EXPECT_FIND("a.*.*.z", "2-3");
    EXPECT_FIND("**", "1-4");
    EXPECT_FIND("a.b.**", "2 4");
    EXPECT_FIND("**.y.z", "3-4");
    EXPECT_FIND("a.**.z", "1-4");
    EXPECT_FIND("a.**.m.**.z", "2-4");
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
