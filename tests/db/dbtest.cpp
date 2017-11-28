// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbtest.cpp - tismet test db
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

    const char dat[] = "test";
    fileRemove("test.tsd");
    fileRemove("test.tsw");
    fileRemove("test.tsl");
    auto h = dbOpen(dat, 128);
    auto stats = dbQueryStats(h);
    EXPECT(stats.pageSize == 128);
    EXPECT(stats.numPages == 2);
    auto pgt = stats.samplesPerPage * 1min;
    uint32_t id;
    unsigned count = 0;
    count += dbInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    MetricInfo info;
    info.retention = duration_cast<Duration>(6.5 * pgt);
    info.interval = 1min;
    dbUpdateMetric(h, id, info);
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
    // add to new page 5. creates sample pages 3, 4, 5, and a radix page
    // to track the value pages.
    dbUpdateSample(h, id, start + 4 * pgt + 10min, 8.0);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 7);
    // add to historical page
    dbUpdateSample(h, id, start - 2min, 1);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 8);
    // circle back onto that historical page, reassigning it's time
    dbUpdateSample(h, id, start + 6 * pgt, 6);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 8);
    EXPECT(stats.freePages == 504);
    EXPECT(stats.metrics == 1);
    // add sample more than the retention period in the future
    dbUpdateSample(h, id, start + 20 * pgt, 1);
    stats = dbQueryStats(h);
    EXPECT(stats.freePages == 508);
    EXPECT(stats.metrics == 1);
    // erase metric
    dbEraseMetric(h, id);
    stats = dbQueryStats(h);
    EXPECT(stats.numPages == 8);
    EXPECT(stats.freePages == 510);
    EXPECT(stats.metrics == 0);

    cout << "----" << endl;
    dbWriteDump(nullptr, cout, h);
    count = 0;
    for (int i = 1; i < 30; ++i) {
        name = "this.is.metric.";
        name += to_string(i);
        uint32_t i2;
        count += dbInsertMetric(i2, h, name);
        dbUpdateSample(h, i2, start, (float) i);
    }
    cout << "metrics inserted: " << count << endl;
    EXPECT(count == 29);

    cout << "----" << endl;
    dbWriteDump(nullptr, cout, h);

    UnsignedSet found;
    dbFindMetrics(found, h, "*.is.*.*5");
    ostringstream os;
    os << found;
    EXPECT(os.str() == "5 15 25");
    cout << "----" << endl;
    dbWriteDump(nullptr, cout, h, "*.is.*.*5");

    for (int i = 100; ; ++i) {
        stats = dbQueryStats(h);
        if (stats.numPages > stats.segmentSize / stats.pageSize)
            break;
        name = "this.is.metric.";
        name += to_string(i);
        uint32_t id;
        count += dbInsertMetric(id, h, name);
        dbUpdateSample(h, id, start, (float) i);
    };

    dbClose(h);

    h = dbOpen(dat);
    EXPECT(h);
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
