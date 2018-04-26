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

const char kVersion[] = "1.0.0";


/****************************************************************************
*
*   ConsoleLogger
*
***/

namespace {

class ConsoleLogger : public ILogNotify {
    void onLog(LogType type, string_view msg) override;

    mutex m_mut;
};

} // namespace

static ConsoleLogger s_consoleLogger;

static struct {
    const char * desc;
    ConsoleAttr attr;
} s_logTypeInfo[] = {
    { "UNKNOWN", kConsoleNormal     }, // invalid
    { "DEBUG",   kConsoleNormal     }, // debug
    { "INFO",    kConsoleHighlight  }, // info
    { "WARN",    kConsoleWarn       }, // warn
    { "ERROR",   kConsoleError      }, // error
    { "FATAL",   kConsoleError      }, // fatal
};
static_assert(size(s_logTypeInfo) == kLogTypes);

//===========================================================================
void ConsoleLogger::onLog(LogType type, string_view msg) {
    auto now = Clock::now();
    Time8601Str nowStr{now, 3, timeZoneMinutes(now)};
    scoped_lock<mutex> lk{m_mut};
    cout << nowStr.view() << ' ';
    if (type >= size(s_logTypeInfo))
        type = kLogTypeInvalid;
    auto & lti = s_logTypeInfo[type];
    if (lti.attr) {
        ConsoleScopedAttr attr(lti.attr);
        cout << lti.desc;
    } else {
        cout << lti.desc;
    }
    cout << ' ';
    cout.write(msg.data(), msg.size());
    cout << endl;
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
    tsDataInitialize();
    if (!appStopping()) {
        evalInitialize(tsDataHandle());
        tsPerfInitialize();
        tsCarbonInitialize();
        tsGraphiteInitialize();
        tsBackupInitialize();
    }
    m_ready = true;
    logMsgInfo() << "Server ready";
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

//===========================================================================
static void app(int argc, char * argv[]) {
    auto ver = "tismet/"s + kVersion;
    Cli cli;
    cli.header(ver + " (" __DATE__ ")");
    cli.versionOpt(kVersion, "tismet");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();

    httpRouteSetDefaultReplyHeader(kHttpServer, ver.c_str());
    httpRouteSetDefaultReplyHeader(kHttpAccessControlAllowOrigin, "*");
    consoleCatchCtrlC();
    if (consoleAttached())
        logMonitor(&s_consoleLogger);
    shutdownMonitor(&s_initTask);
    taskPushCompute(&s_initTask);
    logMsgInfo() << "Server starting";
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
