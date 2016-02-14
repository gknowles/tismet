// timer.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
//using namespace std::rel_ops;


/****************************************************************************
*
*   Incomplete public types
*
***/

class DimTimer {
public:
    static void Update (
        IDimTimerNotify * notify, 
        Duration wait, 
        bool onlyIfSooner
    );
    static void StopSync (IDimTimerNotify * notify);

    DimTimer (IDimTimerNotify * notify);

    // is the notify still pointing back at this timer?
    bool Connected () const;

    IDimTimerNotify * notify{nullptr};
    TimePoint expiration{TimePoint::max()};
    unsigned instance{0};

    bool bugged{false};
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

struct TimerQueueNode {
    shared_ptr<DimTimer> timer;
    TimePoint expiration;
    unsigned instance;

    TimerQueueNode (shared_ptr<DimTimer> & timer);
    bool operator< (const TimerQueueNode & right) const;
    bool operator> (const TimerQueueNode & right) const;
    bool operator== (const TimerQueueNode & right) const;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static mutex s_mut;
static condition_variable s_modeCv; // when run mode changes to stopped
static RunMode s_mode{kRunStopped};
static condition_variable s_queueCv; // when wait for next timer is reduced
static priority_queue<
    TimerQueueNode, 
    vector<TimerQueueNode>, 
    greater<TimerQueueNode>
> s_timers;
static bool s_processing; // dispatch task has been queued and isn't done

static thread::id s_processingThread; // thread running any current callback
static condition_variable s_processingCv; // when running callback completes
static IDimTimerNotify * s_processingNotify; // callback currently in progress


/****************************************************************************
*
*   Queue and run timers
*
***/

//===========================================================================
class CRunTimers : public IDimTaskNotify {
    void OnTask () override;
};
static CRunTimers s_runTimers;

//===========================================================================
void CRunTimers::OnTask () {
    Duration wait;
    TimePoint now{DimClock::now()};
    unique_lock<mutex> lk{s_mut};
    assert(s_processing);
    s_processingThread = this_thread::get_id();
    for (;;) {
        // find next expired timer with notifier to call
        wait = s_timers.empty()
            ? DIM_TIMER_INFINITE
            : s_timers.top().expiration - now;
        if (wait > 0ms) {
            s_processingThread = {};
            s_processing = false;
            break;
        }
        TimerQueueNode node = s_timers.top();
        s_timers.pop();
        if (node.instance != node.timer->instance) 
            continue;
        
        // call notifier
        DimTimer * timer = node.timer.get();
        timer->expiration = TimePoint::max();
        s_processingNotify = timer->notify;
        lk.unlock();
        wait = timer->notify->OnTimer(now);

        // update timer
        lk.lock();
        now = DimClock::now();
        s_processingNotify = nullptr;
        if (!timer->Connected()) {
            s_processingCv.notify_all();
            continue;
        }
        if (wait == DIM_TIMER_INFINITE) 
            continue;
        TimePoint expire = now + wait;
        if (expire < timer->expiration) {
            timer->expiration = expire;
            timer->instance += 1;
            s_timers.push(TimerQueueNode{node.timer});
        }
    }

    if (wait != DIM_TIMER_INFINITE) 
        s_queueCv.notify_one();
}

//===========================================================================
static void TimerQueueThread () {
    for (;;) {
        {
            unique_lock<mutex> lk{s_mut};
            for (;;) {
                if (s_mode == kRunStopping) {
                    while (!s_timers.empty()) 
                        s_timers.pop();
                    s_mode = kRunStopped;
                    s_modeCv.notify_one();
                    return;
                }
                if (s_processing || s_timers.empty()) {
                    s_queueCv.wait(lk);
                    continue;
                }
                Duration wait = s_timers.top().expiration - DimClock::now();
                if (wait <= 0ms) {
                    s_processing = true;
                    break;
                }

                s_queueCv.wait_for(lk, wait);
            }
        }

        DimTaskPushEvent(s_runTimers);
    }
}


/****************************************************************************
*
*   IDimTimerNotify
*
***/

//===========================================================================
IDimTimerNotify::~IDimTimerNotify () {
    if (m_timer)
        DimTimerStopSync(this);
}


/****************************************************************************
*
*   TimerQueueNode
*
***/

//===========================================================================
TimerQueueNode::TimerQueueNode (shared_ptr<DimTimer> & timer)
    : timer{timer}
    , expiration{timer->expiration}
    , instance{timer->instance}
{}

//===========================================================================
bool TimerQueueNode::operator< (const TimerQueueNode & right) const {
    return expiration < right.expiration;
}

//===========================================================================
bool TimerQueueNode::operator> (const TimerQueueNode & right) const {
    return expiration > right.expiration;
}

//===========================================================================
bool TimerQueueNode::operator== (const TimerQueueNode & right) const {
    return expiration == right.expiration
        && timer == right.timer
        && instance == right.instance
    ;
}


/****************************************************************************
*
*   DimTimer
*
***/

//===========================================================================
// static
void DimTimer::Update (
    IDimTimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    TimePoint now{DimClock::now()};
    auto expire = wait == DIM_TIMER_INFINITE
        ? TimePoint::max()
        : now + wait;
    
    {
        lock_guard<mutex> lk{s_mut};
        if (!notify->m_timer) 
            new DimTimer{notify};
        auto & timer = notify->m_timer;
        if (onlyIfSooner && !(expire < timer->expiration))
            return;
        timer->expiration = expire;
        timer->instance += 1;
        if (expire != TimePoint::max()) {
            TimerQueueNode node(timer);
            s_timers.push(node);
            if (!(node == s_timers.top())) 
                return;
        }
    }

    s_queueCv.notify_one();
}

//===========================================================================
// static
void DimTimer::StopSync (IDimTimerNotify * notify) {
    if (!notify->m_timer)
        return;
        
    // if we've stopped just remove the timer, this could be a call from the
    // destructor of a static notify so s_mut may already be destroyed.
    if (s_mode == kRunStopped) {
        notify->m_timer.reset();
        return;
    }

    unique_lock<mutex> lk{s_mut};
    shared_ptr<DimTimer> timer{std::move(notify->m_timer)};
    timer->instance += 1;
    if (this_thread::get_id() == s_processingThread)
        return;
    
    while (notify == s_processingNotify)
        s_processingCv.wait(lk);
}

//===========================================================================
DimTimer::DimTimer (IDimTimerNotify * notify) 
    : notify(notify)
{
    assert(!notify->m_timer);
    notify->m_timer.reset(this);
}

//===========================================================================
bool DimTimer::Connected () const {
    return this == notify->m_timer.get();
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimTimerInitialize () {
    assert(s_mode == kRunStopped);
    s_mode = kRunRunning;
    thread thr{TimerQueueThread};
    thr.detach();
}

//===========================================================================
void IDimTimerDestroy () {
    {
        lock_guard<mutex> lk{s_mut};
        assert(s_mode == kRunRunning);
        s_mode = kRunStopping;
    }
    s_queueCv.notify_one();

    unique_lock<mutex> lk{s_mut};
    while (s_mode != kRunStopped)
        s_modeCv.wait(lk);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void DimTimerUpdate (
    IDimTimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    DimTimer::Update(notify, wait, onlyIfSooner);
}

//===========================================================================
void DimTimerStopSync (IDimTimerNotify * notify) {
    DimTimer::StopSync(notify);
}
