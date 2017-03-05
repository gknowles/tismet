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

class MainShutdown : public IAppShutdownNotify {
    void onAppStartClientCleanup () override;
    bool onAppQueryClientDestroy () override;
};
static MainShutdown s_cleanup;

//===========================================================================
void MainShutdown::onAppStartClientCleanup () {
}

//===========================================================================
bool MainShutdown::onAppQueryClientDestroy () {
    return true;
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
    appMonitorShutdown(&s_cleanup);
    Cli cli;
    cli.header("load v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);
    auto & dat = cli.opt<string>("[dat file]", "metrics.dat");
    if (!cli.parse(m_argc, m_argv))
        return appSignalUsageError();

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
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);

    Application app;
    int code = appRun(app, argc, argv);
    return code;
}
