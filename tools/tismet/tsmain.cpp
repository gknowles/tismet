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
*   ConsoleLogger
*
***/

namespace {

class ConsoleLogger : public ILogNotify {
    void onLog(LogType type, string_view msg) override;
};

} // namespace

static ConsoleLogger s_consoleLogger;

//===========================================================================
void ConsoleLogger::onLog(LogType type, string_view msg) {
    auto now = Clock::now();
    Time8601Str nowStr{now, 3, timeZoneMinutes(now)};
    cout << nowStr.view() << ' ';
    if (type == kLogTypeCrash) {
        ConsoleScopedAttr attr(kConsoleError);
        cout << "CRASH";
    } else if (type == kLogTypeError) {
        ConsoleScopedAttr attr(kConsoleError);
        cout << "ERROR";
    } else if (type == kLogTypeInfo) {
        ConsoleScopedAttr attr(kConsoleHighlight);
        cout << "INFO";
    } else if (type == kLogTypeDebug) {
        cout << "DEBUG";
    } else {
        cout << "UNKNOWN";
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
        tsPerfInitialize();
        tsCarbonInitialize();
        tsGraphiteInitialize();
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
    Cli cli;
    cli.header("tismet v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion, "tismet");
    if (!cli.parse(argc, argv))
        return appSignalUsageError();

    consoleCatchCtrlC();
    if (consoleAttached())
        logMonitor(&s_consoleLogger);
    shutdownMonitor(&s_initTask);
    taskPushCompute(s_initTask);
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
