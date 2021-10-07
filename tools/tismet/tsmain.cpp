// Copyright Glen Knowles 2015 - 2018.
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



/****************************************************************************
*
*   ConsoleLogger
*
***/

namespace {

class ConsoleLogger : public ILogNotify {
    void onLog(const LogMsg & log) override;

    mutex m_mut;
};

} // namespace

static ConsoleLogger s_consoleLogger;

static struct {
    char const * desc;
    ConsoleAttr attr;
} s_logTypeInfo[] = {
    { "UNKNOWN", kConsoleNormal }, // invalid
    { "DEBUG",   kConsoleNormal }, // debug
    { "INFO",    kConsoleNote   }, // info
    { "WARN",    kConsoleWarn   }, // warn
    { "ERROR",   kConsoleError  }, // error
    { "FATAL",   kConsoleError  }, // fatal
};
static_assert(size(s_logTypeInfo) == kLogTypes);

//===========================================================================
void ConsoleLogger::onLog(const LogMsg & log) {
    if (log.type < appLogLevel())
        return;

    auto now = timeNow();
    Time8601Str nowStr{now, 3};
    scoped_lock lk{m_mut};
    cout << nowStr.view() << ' ';
    auto type = log.type < size(s_logTypeInfo)
        ? log.type
        : kLogTypeInvalid;
    auto & lti = s_logTypeInfo[type];
    if (lti.attr) {
        ConsoleScopedAttr attr(lti.attr);
        cout << lti.desc;
    } else {
        cout << lti.desc;
    }
    cout << ' ' << log.msg << endl;
}


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
    void onShutdownConsole(bool firstTry) override;

    atomic<bool> m_ready{false};
};

} // namespace

static InitializeTask s_initTask;

//===========================================================================
void InitializeTask::onTask() {
    winTlsInitialize();
    appTlsInitialize();
    tsWebInitialize();
    tsDataInitialize();
    if (!appStopping()) {
        evalInitialize(tsDataHandle());
        tsPerfInitialize();
        tsCarbonInitialize();
        tsGraphiteInitialize();
        tsBackupInitialize();
        logMsgInfo() << "Server ready";
    }
    m_ready = true;
}

//===========================================================================
void InitializeTask::onShutdownClient(bool firstTry) {
    if (firstTry)
        logMsgInfo() << "Server stopping";
    if (!m_ready)
        shutdownIncomplete();
}

//===========================================================================
void InitializeTask::onShutdownConsole(bool firstTry) {
    logMsgInfo() << "Server stopped";
    logMonitorClose(&s_consoleLogger);
}


/****************************************************************************
*
*   Application
*
***/

static string s_product = "tismet"s;
static string s_version;
static string s_productVersion;

//===========================================================================
static bool serveCmd(Cli & cli) {
    httpRouteSetDefaultReplyHeader(kHttpServer, s_productVersion.c_str());
    httpRouteSetDefaultReplyHeader(kHttpAccessControlAllowOrigin, "*");
    consoleCatchCtrlC();
    if (consoleAttached())
        logMonitor(&s_consoleLogger);

    shutdownMonitor(&s_initTask);
    taskPushCompute(&s_initTask);
    logMsgInfo() << "Server starting";
    return cli.fail(EX_PENDING, "");
}

//===========================================================================
static void app(int argc, char * argv[]) {
    auto vi = envExecVersion();
    ostringstream os;
    os << vi.major << '.' << vi.minor << '.' << vi.patch;
    s_version = os.str();
    s_productVersion = s_product + "/" + s_version;

    Cli cli;
    cli.header(s_productVersion + " (" __DATE__ ")")
        .helpCmd();
    cli.versionOpt(s_version, s_product);
    cli.before([](auto & cli, auto & args) {
        if (args.size() == 1)
            args.push_back((appFlags() & fAppIsService) ? "serve" : "help");
        return true;
    });
    cli.opt<unsigned>("console")
        .show(false).desc("Attach to console of other process.")
        .after([](auto & cli, auto & opt, auto & val) {
            return !opt || consoleAttach(*opt);
        });
    cli.command("serve")
        .desc("Run Tismet server and process requests.")
        .action(serveCmd);

    (void) cli.exec(argc, argv);
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

    int code = appRun(app, argc, argv, fAppServer);
    return code;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
string_view tsProductVersion() {
    return s_productVersion;
}
