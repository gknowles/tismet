// timer.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
//using namespace std::rel_ops;

namespace Dim {


/****************************************************************************
*
*   Incomplete public types
*
***/

class Dim::Timer {
public:
    static void update (
        ITimerNotify * notify, 
        Duration wait, 
        bool onlyIfSooner
    );
    static void stopSync (ITimerNotify * notify);

    Timer (ITimerNotify * notify);

    // is the notify still pointing back at this timer?
    bool connected () const;

    ITimerNotify * notify{nullptr};
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
    shared_ptr<Timer> timer;
    TimePoint expiration;
    unsigned instance;

    TimerQueueNode (shared_ptr<Timer> & timer);
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
static ITimerNotify * s_processingNotify; // callback currently in progress


/****************************************************************************
*
*   Queue and run timers
*
***/

//===========================================================================
class CRunTimers : public ITaskNotify {
    void onTask () override;
};
static CRunTimers s_runTimers;

//===========================================================================
void CRunTimers::onTask () {
    Duration wait;
    TimePoint now{Clock::now()};
    unique_lock<mutex> lk{s_mut};
    assert(s_processing);
    s_processingThread = this_thread::get_id();
    for (;;) {
        // find next expired timer with notifier to call
        wait = s_timers.empty()
            ? kTimerInfinite
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
        Timer * timer = node.timer.get();
        timer->expiration = TimePoint::max();
        s_processingNotify = timer->notify;
        lk.unlock();
        wait = timer->notify->onTimer(now);

        // update timer
        lk.lock();
        now = Clock::now();
        s_processingNotify = nullptr;
        if (!timer->connected()) {
            s_processingCv.notify_all();
            continue;
        }
        if (wait == kTimerInfinite) 
            continue;
        TimePoint expire = now + wait;
        if (expire < timer->expiration) {
            timer->expiration = expire;
            timer->instance += 1;
            s_timers.push(TimerQueueNode{node.timer});
        }
    }

    if (wait != kTimerInfinite) 
        s_queueCv.notify_one();
}

//===========================================================================
static void timerQueueThread () {
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
                Duration wait = s_timers.top().expiration - Clock::now();
                if (wait <= 0ms) {
                    s_processing = true;
                    break;
                }

                s_queueCv.wait_for(lk, wait);
            }
        }

        taskPushEvent(s_runTimers);
    }
}


/****************************************************************************
*
*   ITimerNotify
*
***/

//===========================================================================
ITimerNotify::~ITimerNotify () {
    if (m_timer)
        timerStopSync(this);
}


/****************************************************************************
*
*   TimerQueueNode
*
***/

//===========================================================================
TimerQueueNode::TimerQueueNode (shared_ptr<Timer> & timer)
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
*   Timer
*
***/

//===========================================================================
// static
void Timer::update (
    ITimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    TimePoint now{Clock::now()};
    auto expire = wait == kTimerInfinite
        ? TimePoint::max()
        : now + wait;
    
    {
        lock_guard<mutex> lk{s_mut};
        if (!notify->m_timer) 
            new Timer{notify};
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
void Timer::stopSync (ITimerNotify * notify) {
    if (!notify->m_timer)
        return;
        
    // if we've stopped just remove the timer, this could be a call from the
    // destructor of a static notify so s_mut may already be destroyed.
    if (s_mode == kRunStopped) {
        notify->m_timer.reset();
        return;
    }

    unique_lock<mutex> lk{s_mut};
    shared_ptr<Timer> timer{std::move(notify->m_timer)};
    timer->instance += 1;
    if (this_thread::get_id() == s_processingThread)
        return;
    
    while (notify == s_processingNotify)
        s_processingCv.wait(lk);
}

//===========================================================================
Timer::Timer (ITimerNotify * notify) 
    : notify(notify)
{
    assert(!notify->m_timer);
    notify->m_timer.reset(this);
}

//===========================================================================
bool Timer::connected () const {
    return this == notify->m_timer.get();
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iTimerInitialize () {
    assert(s_mode == kRunStopped);
    s_mode = kRunRunning;
    thread thr{timerQueueThread};
    thr.detach();
}

//===========================================================================
void iTimerDestroy () {
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
void timerUpdate (
    ITimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    Timer::update(notify, wait, onlyIfSooner);
}

//===========================================================================
void timerStopSync (ITimerNotify * notify) {
    Timer::stopSync(notify);
}

} // namespace
