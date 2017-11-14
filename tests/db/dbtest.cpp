// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbtest.cpp - tismet test db
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
static int internalTest() {
    int line = 0;

    TimePoint start = Clock::from_time_t(900'000'000);
    string name = "this.is.metric.1";

    const char dat[] = "test.dat";
    fs::remove(dat);
    auto h = dbOpen(dat, 128);
    auto stats = dbQueryStats(h);
    EXPECT(stats.pageSize == 128);
    EXPECT(stats.numPages == 2);
    auto pgt = stats.samplesPerPage * 1min;
    uint32_t id;
    unsigned count = 0;
    count += dbInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    dbUpdateSample(h, id, start, 1.0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 4);
    dbWriteDump(nullptr, cout, h);
    dbClose(h);
    EXPECT(count == 1);

    h = dbOpen(dat);
    count = dbInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    EXPECT(count == 0);
    dbUpdateSample(h, id, start, 3.0);
    dbUpdateSample(h, id, start + 1min, 4.0);
    dbUpdateSample(h, id, start - 1min, 2.0);
    // add to start of new page 2
    dbUpdateSample(h, id, start + pgt - 1min, 5.0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 5);
    // another on page 2
    dbUpdateSample(h, id, start + pgt, 6.0);
    dbWriteDump(nullptr, cout, h);
    dbClose(h);

    h = dbOpen(dat);
    count = dbInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    EXPECT(count == 0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 5);
    // add to very end of page 2
    dbUpdateSample(h, id, start + 2 * pgt - 2min, 7.0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 5);
    dbWriteDump(nullptr, cout, h);
    // add to new page 5. creates values pages 3, 4, 5, and a radix page 
    // to track the value pages.
    dbUpdateSample(h, id, start + 4 * pgt + 10min, 8.0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 7);
    
    cout << "----" << endl; 
    dbWriteDump(nullptr, cout, h);
    count = 0;
    for (int i = 2; i < 30; ++i) {
        name = "this.is.metric.";
        name += to_string(i);
        uint32_t i2;
        count += dbInsertMetric(i2, h, name);
        dbUpdateSample(h, i2, start, (float) i);
    }
    cout << "metrics inserted: " << count << endl;
    EXPECT(count == 28);

    cout << "----" << endl; 
    dbWriteDump(nullptr, cout, h);

    UnsignedSet found;
    dbFindMetrics(found, h, "*.is.*.*5");
    ostringstream os;
    os << found;
    EXPECT(os.str() == "5 15 25");
    cout << "----" << endl; 
    dbWriteDump(nullptr, cout, h, "*.is.*.*5");

    dbClose(h);

    return EX_OK;
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
