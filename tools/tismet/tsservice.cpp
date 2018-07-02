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
    vector<string> args;

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
static bool execElevated(string_view prog, const vector<string> & args) {
    CharBuf tmp;
    tmp += "--console=";
    tmp += StrFrom<unsigned>(envProcessId());
    for (auto && arg : args) {
        if (&arg == args.data())
            continue;
        tmp += " \"";
        tmp += arg;
        tmp += '"';
    }
    return execElevated(prog, tmp.view()) != -1;
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
    cli.before([&](auto & cli, auto & args) {
        this->args = args; return true;
    });
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

    auto success = false;

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = winSvcInstall(
            nullptr,
            "Tismet",
            "Tismet Server",
            "Provides efficient storage, processing, and access to time series "
                "metrics for graphing and monitoring applications.",
            { "Tcpip", "Afd" },
            "NT Service\\Tismet",
            nullptr
        );
        break;
    case kEnvUserRestrictedAdmin:
        success = execElevated(envExecPath(), s_opts.args);
        break;
    case kEnvUserStandard:
        logMsgError() << "You must be an administrator to create services.";
        break;
    }

    logMonitorClose(consoleBasicLogger());
    if (!success)
        return cli.fail(EX_OSERR, "Unable to create service.");

    return true;
}
