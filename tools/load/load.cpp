// load.cpp - tismet load
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

const char kVersion[] = "1.0";


/****************************************************************************
*
*   MainShutdown
*
***/

class MainShutdown : public IShutdownNotify {
    void onShutdownClient (bool firstTry) override;
};
static MainShutdown s_cleanup;

//===========================================================================
void MainShutdown::onShutdownClient (bool firstTry) {
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
    shutdownMonitor(&s_cleanup);
    Cli cli;
    cli.header("load v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);
    auto & dat = cli.opt<string>("[dat file]", "metrics.dat");
    if (!cli.parse(m_argc, m_argv))
        return appSignalUsageError();

    auto h = tsdOpen(*dat);
    uint32_t id;
    unsigned count = 0;
    count += tsdInsertMetric(id, h, "this.is.metric.1");
    cout << "metrics inserted: " << count << endl;
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
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);

    Application app;
    int code = appRun(app, argc, argv);
    return code;
}
