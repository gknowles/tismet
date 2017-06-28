// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tismet.cpp - tismet
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
*   Run server
*
***/

//===========================================================================
static bool startCmd(Cli & cli) {
    consoleEnableCtrlC();
    logMsgInfo() << "Server started";
    return true;
}


/****************************************************************************
*     
*   Application
*     
***/

namespace {
class Application : public IAppNotify {
    void onAppRun () override;
};
} // namespace

//===========================================================================
void Application::onAppRun () {
    Cli cli;
    cli.header("tismet v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);
    cli.command("start").desc("Start server")
        .action(startCmd);

    if (m_argc == 1) {
        auto os = logMsgInfo();
        return appSignalShutdown(cli.printHelp(os));
    }
    if (!cli.parse(m_argc, m_argv) || !cli.exec())
        return appSignalUsageError();
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
