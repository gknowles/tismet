// load.cpp - tismet load
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


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
class Application : public ITaskNotify {
    void onTask () override;
};
} // namespace

//===========================================================================
void Application::onTask () {
    appMonitorShutdown(&s_cleanup);

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
    int code = appRun(app);
    return code;
}
