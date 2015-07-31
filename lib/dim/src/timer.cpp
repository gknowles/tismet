// timer.cpp - dim core
#include "dim/task.h"
#include "dim/timer.h"
#include "intern.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

using namespace std;
//using namespace std::rel_ops;


/****************************************************************************
*
*   Incomplete public types
*
***/

class Timer {
public:
    static void Update (
        ITimerNotify * notify, 
        Duration wait, 
        bool onlyIfSooner
    );
    static void StopSync (ITimerNotify * notify);

    Timer (ITimerNotify * notify);
    bool Connected () const;

    ITimerNotify * notify{nullptr};
    TimePoint expiration;
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
    bool operator== (const TimerQueueNode & right) const;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static mutex s_mut;
static condition_variable s_queueCv; // when wait for next timer is reduced
static condition_variable s_modeCv; // when run mode changes to stopped
static ERunMode s_mode{MODE_STOPPED};
static priority_queue<TimerQueueNode> s_timers;
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
    void OnTask () override;
};
static CRunTimers s_runTimers;

//===========================================================================
void CRunTimers::OnTask () {
    Duration wait;
    TimePoint now{Clock::now()};
    unique_lock<mutex> lk{s_mut};
    assert(s_processing);
    s_processingThread = this_thread::get_id();
    for (;;) {
        // find next expired timer with notifier to call
        wait = s_timers.empty()
            ? TIMER_WAIT_FOREVER
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
        wait = timer->notify->OnTimer();

        // update timer
        lk.lock();
        now = Clock::now();
        s_processingNotify = nullptr;
        if (!timer->Connected()) {
            s_processingCv.notify_all();
            continue;
        }
        if (wait == TIMER_WAIT_FOREVER) 
            continue;
        TimePoint expire = now + wait;
        if (expire < timer->expiration) {
            timer->expiration = expire;
            timer->instance += 1;
            s_timers.push(TimerQueueNode{node.timer});
        }
    }

    if (wait != TIMER_WAIT_FOREVER) 
        s_queueCv.notify_one();
}

//===========================================================================
static void TimerQueueThread () {
    // Wait is initialized to 0ms, bypassing the condition the first time 
    // through. This is to allow for a timer being queued before we first 
    // take the mutex.
    Duration wait = 1ms;
    for (;;) {
        {
            unique_lock<mutex> lk{s_mut};
            for (;;) {
                if (s_mode == MODE_STOPPING) {
                    while (!s_timers.empty()) 
                        s_timers.pop();
                    s_mode = MODE_STOPPED;
                    s_modeCv.notify_one();
                    return;
                }
                if (s_processing || s_timers.empty()) {
                    s_queueCv.wait(lk);
                    continue;
                }
                wait = s_timers.top().expiration - Clock::now();
                if (wait <= 0ms) {
                    s_processing = true;
                    break;
                }

                s_queueCv.wait_for(lk, wait);
            }
        }

        TaskPushEvent(s_runTimers);
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
        TimerStopSync(this);
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
void Timer::Update (
    ITimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    TimePoint now{Clock::now()};
    auto expire = wait == TIMER_WAIT_FOREVER
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
void Timer::StopSync (ITimerNotify * notify) {
    if (!notify->m_timer)
        return;
        
    // if we've stopped just removed the timer, this could be a call from the
    // destructor of a static notify so s_mut may already be destroyed.
    if (s_mode == MODE_STOPPED) {
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
bool Timer::Connected () const {
    return this == notify->m_timer.get();
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void ITimerInitialize () {
    assert(s_mode == MODE_STOPPED);
    s_mode = MODE_RUNNING;
    thread thr{TimerQueueThread};
    thr.detach();
}

//===========================================================================
void ITimerDestroy () {
    {
        lock_guard<mutex> lk{s_mut};
        assert(s_mode == MODE_RUNNING);
        s_mode = MODE_STOPPING;
    }
    s_queueCv.notify_one();

    unique_lock<mutex> lk{s_mut};
    while (s_mode != MODE_STOPPED)
        s_modeCv.wait(lk);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void TimerUpdate (
    ITimerNotify * notify,
    Duration wait,
    bool onlyIfSooner
) {
    Timer::Update(notify, wait, onlyIfSooner);
}

//===========================================================================
void TimerStopSync (ITimerNotify * notify) {
    Timer::StopSync(notify);
}
