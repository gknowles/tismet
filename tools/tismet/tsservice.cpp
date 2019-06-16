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
static bool execElevated(string_view prog, vector<string> const & rawArgs) {
    auto args = rawArgs;
    args[0] = "--console=";
    args[0] += StrFrom<unsigned>(envProcessId());
    auto argline = Cli::toCmdline(args);
    int ec;
    return execElevatedWait(&ec, prog, argline);
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
    WinServiceConfig sconf;
    sconf.serviceName = "Tismet";
    sconf.displayName = "Tismet Server",
    sconf.desc = "Provides efficient storage, processing, and access to time "
        "series metrics for graphing and monitoring applications.";
    sconf.deps = { "Tcpip", "Afd" };
    sconf.account = "NT Service\\Tismet";
    sconf.sidType = WinServiceConfig::SidType::kUnrestricted;
    sconf.privs = {
        "SeChangeNotifyPrivilege",
        // "SeManageVolumePrivilege",   // SetFileValidData
        // "SeLockMemoryPrivilege",     // VirtualAlloc with MEM_LARGE_PAGES
    };
    sconf.failureFlag = WinServiceConfig::FailureFlag::kCrashOrNonZeroExitCode;
    sconf.failureReset = 24h;
    sconf.failureActions = {
        { WinServiceConfig::Action::kRestart, 10s },
        { WinServiceConfig::Action::kRestart, 60s },
        { WinServiceConfig::Action::kRestart, 10min },
    };

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = winSvcCreate(sconf);
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
