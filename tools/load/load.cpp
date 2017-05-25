// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// load.cpp - tismet load
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

const char kVersion[] = "1.0";


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static int internalTest() {
    TimePoint start = Clock::from_time_t(900'000'000);
    string name = "this.is.metric.1";

    const char dat[] = "test.dat";
    fs::remove(dat);
    auto h = tsdOpen(dat, 128);
    uint32_t id;
    unsigned count = 0;
    count += tsdInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    tsdWriteData(h, id, start, 1.0);
    tsdDump(cout, h);
    tsdClose(h);

    h = tsdOpen(dat);
    count = tsdInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    tsdWriteData(h, id, start, 3.0);
    tsdWriteData(h, id, start + 1min, 4.0);
    tsdWriteData(h, id, start - 1min, 2.0);
    tsdWriteData(h, id, start + 20min, 5.0);
    tsdWriteData(h, id, start + 21min, 6.0);
    tsdDump(cout, h);
    tsdClose(h);

    h = tsdOpen(dat);
    count = tsdInsertMetric(id, h, name);
    cout << "metrics inserted: " << count << endl;
    tsdWriteData(h, id, start + 40min, 7.0);
    tsdDump(cout, h);
    tsdWriteData(h, id, start + 100min, 8.0);
    cout << "----" << endl; tsdDump(cout, h);
    count = 0;
    for (int i = 2; i < 30; ++i) {
        name = "this.is.metric.";
        name += to_string(i);
        uint32_t i2;
        count += tsdInsertMetric(i2, h, name);
        tsdWriteData(h, i2, start, (float) i);
    }
    cout << "metrics inserted: " << count << endl;
    cout << "----" << endl; tsdDump(cout, h);
    tsdClose(h);

    return EX_OK;
}


/****************************************************************************
*     
*   Application
*     
***/

namespace {
class Application : public IAppNotify {
    void onAppRun() override;
};
} // namespace

//===========================================================================
void Application::onAppRun () {
    Cli cli;
    cli.header("load v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);
    auto & dat = cli.opt<string>("[dat file]", "metrics.dat");
    auto & test = cli.opt<bool>("test", true).desc("Run internal unit tests");
    if (!cli.parse(m_argc, m_argv))
        return appSignalUsageError();
    if (*test)
        return appSignalShutdown(internalTest());

    auto h = tsdOpen(*dat);
    tsdClose(h);

    appSignalShutdown(EX_OK);
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

    Application app;
    int code = appRun(app, argc, argv);
    return code;
}
