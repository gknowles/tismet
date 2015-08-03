// app.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CleanupInfo {
    IDimAppShutdownNotify * notify;
    bool destroyed = false;

    CleanupInfo (IDimAppShutdownNotify * notify) : notify(notify) {}
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

class MainTimer : public IDimTimerNotify {
public:
    typedef void (IDimAppShutdownNotify::*CleanFn)();
    typedef bool (IDimAppShutdownNotify::*QueryFn)();

public:
    bool Stopped () const;
    bool QueryDestroyFailed (Duration grace);

    // IDimTimerNotify
    Duration OnTimer () override;

private:
    void StartCleanup (CleanFn notify);
    bool QueryDestroy (QueryFn notify);

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
static unsigned s_exitcode;

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
Duration MainTimer::OnTimer () {
    bool next = true;
    switch (m_mode) {
        case MAIN_SC: 
            s_runMode = kRunStopping;
            m_shutdownStart = DimClock::now();
            break;
        case MAIN_QD:
            break;
        case CLIENT_SC:
            StartCleanup(&IDimAppShutdownNotify::OnAppStartClientCleanup);
            break;
        case CLIENT_QD:
            next = QueryDestroy(&IDimAppShutdownNotify::OnAppQueryClientDestroy);
            break;
        case SERVER_SC:
            StartCleanup(&IDimAppShutdownNotify::OnAppStartServerCleanup);
            break;
        case SERVER_QD:
            next = QueryDestroy(&IDimAppShutdownNotify::OnAppQueryServerDestroy);
            break;
        case CONSOLE_SC:
            StartCleanup(&IDimAppShutdownNotify::OnAppStartConsoleCleanup);
            break;
        case CONSOLE_QD:
            next = QueryDestroy(&IDimAppShutdownNotify::OnAppQueryConsoleDestroy);
            break;
        case DONE:
            s_cleaners.clear();
            s_stopped.notify_one();
            return DIM_TIMER_INFINITE;
    }

    // some delay when rerunning the same step (i.e. QueryDestroy failed)
    if (!next)
        return 10ms;

    m_mode = ETimerMode(m_mode + 1);
    return 0ms;
}

//===========================================================================
bool MainTimer::Stopped() const {
    return m_mode == DONE;
}

//===========================================================================
bool MainTimer::QueryDestroyFailed (Duration grace) {
    if (DimClock::now() - m_shutdownStart > s_shutdownTimeout + grace) {
        assert(0 && "shutdown timeout");
        terminate();
    }
    return false;
}

//===========================================================================
void MainTimer::StartCleanup (CleanFn notify) {
    for (auto&& v : s_cleaners) {
        (v.notify->*notify)();
        v.destroyed = false;
    }
}

//===========================================================================
bool MainTimer::QueryDestroy (QueryFn notify) {
    for (auto&& v : s_cleaners) {
        if (!v.destroyed) {
            if ((v.notify->*notify)()) {
                v.destroyed = true;
            } else {
                return QueryDestroyFailed(5s);
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
void DimAppInitialize () {
    IDimTaskInitialize();
    IDimTimerInitialize();
    IDimSocketInitialize();
    s_runMode = kRunRunning;
}

//===========================================================================
void DimAppSignalShutdown (int exitcode) {
    s_exitcode = exitcode;
    s_mainTimer = {};
    DimTimerUpdate(&s_mainTimer, 0ms);
}

//===========================================================================
int DimAppWaitForShutdown () {
    unique_lock<mutex> lk{s_runMut};
    while (!s_mainTimer.Stopped())
        s_stopped.wait(lk);
    IDimTimerDestroy();
    IDimTaskDestroy();
    s_runMode = kRunStopped;
    return s_exitcode;
}

//===========================================================================
void DimAppMonitorShutdown (IDimAppShutdownNotify * cleaner) {
    s_cleaners.emplace(s_cleaners.begin(), cleaner);
}

//===========================================================================
bool DimQueryDestroyFailed () {
    return s_mainTimer.QueryDestroyFailed(0ms);
}

//===========================================================================
RunMode DimAppMode () {
    return s_runMode;
}
