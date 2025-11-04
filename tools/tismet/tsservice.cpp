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
static ExecResult execElevated(const vector<string> & rawArgs) {
    ExecResult out;
    auto args = rawArgs;
    args[0] = Path(envExecPath()).relative(appRootDir());
    string arg1 = "--console=";
    arg1 += toChars<unsigned>(envProcessId()).view();
    args.insert(args.begin() + 1, arg1);
    auto argline = Cli::toCmdline(args);
    execElevatedWait(&out, argline);
    return out;
}


/****************************************************************************
*
*   Action - contains code common to command actions
*
***/

namespace {

class Action {
public:
    enum Flags : unsigned {
        fSuccess    = 1 << 0,   // operation was successful
        fReported   = 1 << 1,   // has been fully reported to console
        fMore       = 1 << 2,   // hasn't been executed yet
        fAlready    = 1 << 3,   // service was already stopped/paused/etc
    };

public:
    Action(Cli & cli, string_view type, string_view typed);
    ~Action();
    explicit operator bool() const { return m_flags.any(fMore); }

    EnumFlags<Flags> m_flags {};

private:
    Cli m_cli;
    string m_type;
    string m_typed;
};

} // namespace

//===========================================================================
Action::Action(Cli & cli, string_view type, string_view typed)
    : m_cli(cli)
    , m_type(type)
    , m_typed(typed)
{
    logMonitor(consoleBasicLogger());
    auto rights = envProcessRights();
    if (rights == kEnvUserAdmin) {
        m_flags.set(fMore);
    } else if (rights == kEnvUserRestrictedAdmin && !tsConsoleOwner()) {
        auto res = execElevated(s_opts.args);
        m_flags.set(fReported, res.exitType == ExecResult::kFinished);
        m_flags.set(fSuccess, !res.exitCode);
        if (res.exitType == ExecResult::kCanceled)
            logMsgError() << "Operation canceled by user.";
    } else if (rights == kEnvUserStandard) {
        logMsgError() << "You must be an administrator to " << type
            << " services.";
    }
};

//===========================================================================
Action::~Action() {
    if (m_flags.any(fReported)) {
        if (!m_flags.any(fSuccess))
            m_cli.fail(EX_OSERR);
    } else {
        if (m_flags.any(fSuccess)) {
            auto os = logMsgInfo();
            os << appServiceName() << " service ";
            if (m_flags.any(fAlready))
                os << "already ";
            os << m_typed << ".";
        } else {
            m_cli.fail(EX_OSERR, "Unable to " + m_type + " service.");
        }
    }
    logMonitorClose(consoleBasicLogger());
}


/****************************************************************************
*
*   Install
*
***/

//===========================================================================
static bool installService() {
    WinSvcConf conf;
    auto cmd = Cli::toCmdline({envExecPath(), "serve"});
    conf.serviceName = appServiceName();
    conf.desc = "Provides efficient storage, processing, and access to time "
        "series metrics for graphing and monitoring applications.";
    conf.progWithArgs = cmd.c_str();
    conf.account = WinSvcConf::kLocalService;
    conf.deps = { "Tcpip", "Afd" };
    conf.sidType = WinSvcConf::SidType::kRestricted;
    conf.privs = {
        "SeChangeNotifyPrivilege",
        // "SeManageVolumePrivilege",   // SetFileValidData
        // "SeLockMemoryPrivilege",     // VirtualAlloc with MEM_LARGE_PAGES
    };
    conf.failureFlag = WinSvcConf::FailureFlag::kCrashOrNonZeroExitCode;
    conf.failureReset = 24h;
    conf.failureActions = {
        { WinSvcConf::Action::kRestart, 10s },
        { WinSvcConf::Action::kRestart, 60s },
        { WinSvcConf::Action::kRestart, 10min },
    };

    return !winSvcCreate(conf);
}

//===========================================================================
static bool setFileAccess() {
    using namespace Dim::File::Access;
    auto path = Path(appRootDir());
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
    Action act(cli, "create", "created");
    if (!act)
        return;

    if (installService()) {
        if (setFileAccess()) {
            act.m_flags.set(Action::fSuccess);
        } else {
            winSvcDelete(appServiceName());
        }
    }
}


/****************************************************************************
*
*   Uninstall
*
***/

//===========================================================================
static void uninstallCmd(Cli & cli) {
    Action act(cli, "delete", "deleted");
    if (!act)
        return;

    if (!winSvcDelete(appServiceName()))
        act.m_flags.set(Action::fSuccess);
}


/****************************************************************************
*
*   Start
*
***/

//===========================================================================
static void startCmd(Cli & cli) {
    Action act(cli, "start", "started");
    if (!act)
        return;

    WinSvcStat st;
    if (!winSvcStart(&st, appServiceName()))
        act.m_flags.set(Action::fSuccess);
    if (st.alreadyInState)
        act.m_flags.set(Action::fAlready);
}


/****************************************************************************
*
*   Stop
*
***/

//===========================================================================
static void stopCmd(Cli & cli) {
    Action act(cli, "stop", "stopped");
    if (!act)
        return;

    WinSvcStat st;
    if (!winSvcStop(&st, appServiceName()))
        act.m_flags.set(Action::fSuccess);
    if (st.alreadyInState)
        act.m_flags.set(Action::fAlready);
}


/****************************************************************************
*
*   Command options
*
***/

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.before([&](auto & cli, auto & args) {
        this->args = args;
    }, 0);
    cli.command("install").action(installCmd)
        .desc("Install as a Windows service.");
    cli.command("uninstall").action(uninstallCmd)
        .desc("Uninstall the service.");
    cli.command("start").action(startCmd)
        .desc("Start the service");
    cli.command("stop").action(stopCmd)
        .desc("Stop the service");
}
