// Copyright Glen Knowles 2018 - 2022.
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
static bool execElevated(const vector<string> & rawArgs) {
    auto args = rawArgs;
    args[0] = Path(envExecPath()).relative(appRootDir());
    string arg1 = "--console=";
    arg1 += toChars<unsigned>(envProcessId()).view();
    args.insert(args.begin() + 1, arg1);
    auto argline = Cli::toCmdline(args);
    int ec;
    return execElevatedWait(&ec, argline);
}


/****************************************************************************
*
*   Command line
*
***/

static void installCmd(Cli & cli);
static void uninstallCmd(Cli & cli);
static void startCmd(Cli & cli);
static void stopCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.before([&](auto & cli, auto & args) {
        this->args = args;
    });
    cli.command("install").action(installCmd)
        .desc("Install as a Windows service.");
    cli.command("uninstall").action(uninstallCmd)
        .desc("Uninstall the service.");
    cli.command("start").action(startCmd)
        .desc("Start the service");
    cli.command("stop").action(stopCmd)
        .desc("Stop the service");
}


/****************************************************************************
*
*   Install
*
***/

//===========================================================================
static bool installService() {
    auto cmd = Cli::toCmdline({envExecPath(), "serve"});
    WinSvcConf sconf;
    sconf.serviceName = appServiceName();
    sconf.desc = "Provides efficient storage, processing, and access to time "
        "series metrics for graphing and monitoring applications.";
    sconf.progWithArgs = cmd.c_str();
    sconf.account = WinSvcConf::kLocalService;
    sconf.deps = { "Tcpip", "Afd" };
    sconf.sidType = WinSvcConf::SidType::kRestricted;
    sconf.privs = {
        "SeChangeNotifyPrivilege",
        // "SeManageVolumePrivilege",   // SetFileValidData
        // "SeLockMemoryPrivilege",     // VirtualAlloc with MEM_LARGE_PAGES
    };
    sconf.failureFlag = WinSvcConf::FailureFlag::kCrashOrNonZeroExitCode;
    sconf.failureReset = 24h;
    sconf.failureActions = {
        { WinSvcConf::Action::kRestart, 10s },
        { WinSvcConf::Action::kRestart, 60s },
        { WinSvcConf::Action::kRestart, 10min },
    };

    return !winSvcCreate(sconf);
}

//===========================================================================
static bool setFileAccess() {
    using namespace Dim::File::Access;
    auto path = Path{envExecPath()}.removeFilename();
    struct {
        const char * path;
        Right allow;
        Inherit inherit = Inherit::kNone;
    } rights[] = {
        { ".",      Right::kReadOnly,   Inherit::kAll },
        { "crash",  Right::kModify },
        { "data",   Right::kModify,     Inherit::kAll },
        { "log",    Right::kModify },
    };
    unsigned failed = 0;
    for (auto&& right : rights) {
        auto rpath = path / right.path;
        if (fileAddAccess(
            rpath,
            "NT SERVICE\\Tismet",
            right.allow,
            right.inherit
        )) {
            logMsgError() << "Unable to set access to '" << rpath << "'";
            failed += 1;
        }
    }
    return !failed;
}

//===========================================================================
static void installCmd(Cli & cli) {
    auto success = false;
    logMonitor(consoleBasicLogger());

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = installService() && setFileAccess();
        break;
    case kEnvUserRestrictedAdmin:
        success = execElevated(s_opts.args);
        break;
    case kEnvUserStandard:
        logMsgError() << "You must be an administrator to create services.";
        break;
    }

    logMonitorClose(consoleBasicLogger());
    if (!success)
        cli.fail(EX_OSERR, "Unable to create service.");
}


/****************************************************************************
*
*   Uninstall
*
***/

//===========================================================================
static bool uninstallService() {
    return false;
}

//===========================================================================
static void uninstallCmd(Cli & cli) {
    auto success = false;
    logMonitor(consoleBasicLogger());

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = uninstallService();
        break;
    case kEnvUserRestrictedAdmin:
        success = execElevated(s_opts.args);
        break;
    case kEnvUserStandard:
        logMsgError() << "You must be an administrator to create services.";
        break;
    }

    logMonitorClose(consoleBasicLogger());
    if (!success)
        cli.fail(EX_OSERR, "Unable to create service.");
}


/****************************************************************************
*
*   Start
*
***/

//===========================================================================
static bool startService() {
    return false;
}

//===========================================================================
static void startCmd(Cli & cli) {
    auto success = false;
    logMonitor(consoleBasicLogger());

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = startService();
        break;
    case kEnvUserRestrictedAdmin:
        success = execElevated(s_opts.args);
        break;
    case kEnvUserStandard:
        logMsgError() << "You must be an administrator to create services.";
        break;
    }

    logMonitorClose(consoleBasicLogger());
    if (!success)
        cli.fail(EX_OSERR, "Unable to create service.");
}


/****************************************************************************
*
*   Stop
*
***/

//===========================================================================
static bool stopService() {
    return false;
}

//===========================================================================
static void stopCmd(Cli & cli) {
    auto success = false;
    logMonitor(consoleBasicLogger());

    switch (envProcessRights()) {
    case kEnvUserAdmin:
        success = stopService();
        break;
    case kEnvUserRestrictedAdmin:
        success = execElevated(s_opts.args);
        break;
    case kEnvUserStandard:
        logMsgError() << "You must be an administrator to create services.";
        break;
    }

    logMonitorClose(consoleBasicLogger());
    if (!success)
        cli.fail(EX_OSERR, "Unable to create service.");
}
