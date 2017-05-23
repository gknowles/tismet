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
    auto start = Clock::now();

    const char dat[] = "test.dat";
    fs::remove(dat);
    auto h = tsdOpen(dat);
    uint32_t id;
    unsigned count = 0;
    count += tsdInsertMetric(id, h, "this.is.metric.1");
    cout << "metrics inserted: " << count << endl;
    tsdWriteData(h, id, start, 1.0);
    tsdClose(h);

    h = tsdOpen(dat);
    count = tsdInsertMetric(id, h, "this.is.metric.1");
    cout << "metrics inserted: " << count << endl;
    tsdWriteData(h, id, start, 2.0);
    tsdWriteData(h, id, start + 1min, 3.0);
    tsdWriteData(h, id, start - 1min, 4.0);
    tsdWriteData(h, id, start + 20min, 5.0);
    tsdWriteData(h, id, start + 21min, 6.0);
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
