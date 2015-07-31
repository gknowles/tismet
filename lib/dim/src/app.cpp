// app.cpp - dim core
#include "dim/app.h"
#include "dim/timer.h"

#include "intern.h"

#include <cassert>
#include <mutex>
#include <vector>

using namespace std;
using namespace std::chrono;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CleanupInfo {
    IAppCleanup * notify;
    bool destroyed = false;

    CleanupInfo (IAppCleanup * notify) : notify(notify) {}
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
    typedef void (IAppCleanup::*CleanFn)();
    typedef bool (IAppCleanup::*QueryFn)();

public:
    bool Stopped () const;
    bool QueryDestroyFailed (Duration grace);

    // ITimerNotify
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
static ERunMode s_runMode{MODE_STOPPED};


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
            s_runMode = MODE_STOPPING;
            m_shutdownStart = Clock::now();
            break;
        case MAIN_QD:
            break;
        case CLIENT_SC:
            StartCleanup(&IAppCleanup::OnAppStartClientCleanup);
            break;
        case CLIENT_QD:
            next = QueryDestroy(&IAppCleanup::OnAppQueryClientDestroy);
            break;
        case SERVER_SC:
            StartCleanup(&IAppCleanup::OnAppStartServerCleanup);
            break;
        case SERVER_QD:
            next = QueryDestroy(&IAppCleanup::OnAppQueryServerDestroy);
            break;
        case CONSOLE_SC:
            StartCleanup(&IAppCleanup::OnAppStartConsoleCleanup);
            break;
        case CONSOLE_QD:
            next = QueryDestroy(&IAppCleanup::OnAppQueryConsoleDestroy);
            break;
        case DONE:
            s_cleaners.clear();
            s_stopped.notify_one();
            return TIMER_WAIT_FOREVER;
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
    if (Clock::now() - m_shutdownStart > s_shutdownTimeout + grace) {
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
void Initialize () {
    ITaskInitialize();
    ITimerInitialize();
    s_runMode = MODE_RUNNING;
}

//===========================================================================
void SignalShutdown (int exitcode) {
    s_exitcode = exitcode;
    s_mainTimer = {};
    TimerUpdate(&s_mainTimer, 0ms);
}

//===========================================================================
int WaitForShutdown () {
    unique_lock<mutex> lk{s_runMut};
    while (!s_mainTimer.Stopped())
        s_stopped.wait(lk);
    ITimerDestroy();
    ITaskDestroy();
    s_runMode = MODE_STOPPED;
    return s_exitcode;
}

//===========================================================================
void RegisterCleanup (IAppCleanup * cleaner) {
    s_cleaners.emplace(s_cleaners.begin(), cleaner);
}

//===========================================================================
bool QueryDestroyFailed () {
    return s_mainTimer.QueryDestroyFailed(0ms);
}

//===========================================================================
ERunMode AppMode () {
    return s_runMode;
}
