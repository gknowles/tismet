// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsmain.cpp - tismet
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
*   Initialize
*
***/

namespace {

class InitializeTask
    : public IShutdownNotify
    , public ITaskNotify
{
    void onTask() override;
    void onShutdownClient(bool firstTry) override;

    atomic<bool> m_ready{false};
};

} // namespace

static InitializeTask s_initTask;

//===========================================================================
void InitializeTask::onTask() {
    tsDataInitialize();
    if (!appStopping())
        tsCarbonInitialize();
    m_ready = true;
}

//===========================================================================
void InitializeTask::onShutdownClient(bool firstTry) {
    if (!m_ready)
        shutdownIncomplete();
}


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    cli.header("tismet v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion, "tismet");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();

    consoleCatchCtrlC();
    shutdownMonitor(&s_initTask);
    taskPushCompute(s_initTask);
    cout << "Server started" << endl;
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

    int code = appRun(app, argc, argv, fAppServer);
    return code;
}
