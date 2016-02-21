// app.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;

namespace Dim {


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CleanupInfo {
    IAppShutdownNotify * notify;
    bool destroyed = false;

    CleanupInfo (IAppShutdownNotify * notify) : notify(notify) {}
};

enum ETimerMode {
    MAIN_SC,
    MAIN_QD,
    CLIENT_SC,
    CLIENT_QD,
    SERVER_SC,
    SERVER_QD,
    CONSOLE_SC,
    CONSOLE_QD,
    DONE
};

class MainTimer : public ITimerNotify {
public:
    typedef void (IAppShutdownNotify::*CleanFn)();
    typedef bool (IAppShutdownNotify::*QueryFn)();

public:
    bool stopped () const;
    bool queryDestroyFailed (Duration grace);

    // ITimerNotify
    Duration onTimer (TimePoint now) override;

private:
    void startCleanup (CleanFn notify);
    bool queryDestroy (QueryFn notify);

    enum ETimerMode m_mode{MAIN_SC};
    const char * m_modeName{nullptr};
    TimePoint m_shutdownStart;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static MainTimer s_mainTimer;
static int s_exitcode;

// cleaners are in the order (newest to oldest) that they will be executed.
static vector<CleanupInfo> s_cleaners;

static Duration s_shutdownTimeout{2min};
static mutex s_runMut;
static condition_variable s_stopped;
static RunMode s_runMode{kRunStopped};


/****************************************************************************
*
*   MainTimer
*
***/

//===========================================================================
Duration MainTimer::onTimer (TimePoint now) {
    bool next = true;
    switch (m_mode) {
        case MAIN_SC: 
            s_runMode = kRunStopping;
            m_shutdownStart = now;
            break;
        case MAIN_QD:
            break;
        case CLIENT_SC:
            startCleanup(&IAppShutdownNotify::onAppStartClientCleanup);
            break;
        case CLIENT_QD:
            next = queryDestroy(&IAppShutdownNotify::onAppQueryClientDestroy);
            break;
        case SERVER_SC:
            startCleanup(&IAppShutdownNotify::onAppStartServerCleanup);
            break;
        case SERVER_QD:
            next = queryDestroy(&IAppShutdownNotify::onAppQueryServerDestroy);
            break;
        case CONSOLE_SC:
            startCleanup(&IAppShutdownNotify::onAppStartConsoleCleanup);
            break;
        case CONSOLE_QD:
            next = queryDestroy(&IAppShutdownNotify::onAppQueryConsoleDestroy);
            break;
        case DONE:
            s_cleaners.clear();
            s_stopped.notify_one();
            return kTimerInfinite;
    }

    // some delay when rerunning the same step (i.e. QueryDestroy failed)
    if (!next)
        return 10ms;

    m_mode = ETimerMode(m_mode + 1);
    return 0ms;
}

//===========================================================================
bool MainTimer::stopped() const {
    return m_mode == DONE;
}

//===========================================================================
bool MainTimer::queryDestroyFailed (Duration grace) {
    if (Clock::now() - m_shutdownStart > s_shutdownTimeout + grace) {
        assert(0 && "shutdown timeout");
        terminate();
    }
    return false;
}

//===========================================================================
void MainTimer::startCleanup (CleanFn notify) {
    for (auto&& v : s_cleaners) {
        (v.notify->*notify)();
        v.destroyed = false;
    }
}

//===========================================================================
bool MainTimer::queryDestroy (QueryFn notify) {
    for (auto&& v : s_cleaners) {
        if (!v.destroyed) {
            if ((v.notify->*notify)()) {
                v.destroyed = true;
            } else {
                return queryDestroyFailed(5s);
            }
        }
    }
    return true;
}


/****************************************************************************
*
*   Externals
*
***/

//===========================================================================
int appRun (ITaskNotify & app) {
    iHashInitialize();
    iTaskInitialize();
    iTimerInitialize();
    iFileInitialize();
    iSocketInitialize();
    s_runMode = kRunRunning;

    taskPushEvent(app);

    unique_lock<mutex> lk{s_runMut};
    while (!s_mainTimer.stopped())
        s_stopped.wait(lk);
    iTimerDestroy();
    iTaskDestroy();
    s_runMode = kRunStopped;
    return s_exitcode;
}

//===========================================================================
void appSignalShutdown (int exitcode) {
    if (exitcode > s_exitcode)
        s_exitcode = exitcode;
    s_mainTimer = {};
    timerUpdate(&s_mainTimer, 0ms);
}

//===========================================================================
void appMonitorShutdown (IAppShutdownNotify * cleaner) {
    s_cleaners.emplace(s_cleaners.begin(), cleaner);
}

//===========================================================================
bool appQueryDestroyFailed () {
    return s_mainTimer.queryDestroyFailed(0ms);
}

//===========================================================================
RunMode appMode () {
    return s_runMode;
}

} // namespace

