// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tsservice.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CmdOpts {
    Path tslfile;
    Path ofile;
    bool all;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool winSvcInstall(
    string_view path,
    string_view name,
    string_view desc,
    string_view deps
) {
    if (path.empty())
        path = envExecPath();
    assert(!name.empty());
    auto h = OpenSCManager(NULL, NULL, SC_MANAGER_CREATE_SERVICE);
    if (!h) {
        logMsgError() << "OpenSCManager(CREATE_SERVICE): " << WinError{};
        return false;
    }
    CloseServiceHandle(h);
    return true;
}


/****************************************************************************
*
*   Command line
*
***/

static bool installCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("install")
        .desc("Install Tismet service.")
        .action(installCmd);
}


/****************************************************************************
*
*   Text command
*
***/

//===========================================================================
static bool installCmd(Cli & cli) {
    logMonitor(consoleBasicLogger());
    auto success = winSvcInstall(
        {}, 
        "Tismet",
        "Provides storage, processing, and access to time series metric data.",
        "Tcpip/Afd"
    );
    logMonitorClose(consoleBasicLogger());

    if (!success) {
        return cli.fail(EX_OSERR, "Unable to create service.");
    }

    return true;
}
