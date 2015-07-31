// task.cpp - dim core
#include "dim/handle.h"
#include "dim/task.h"
#include "intern.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace std;
using namespace std::rel_ops;


/****************************************************************************
*
*   Incomplete public types
*
***/

class TaskQueue {
public:
    HTaskQueue hq;
    string name;

    // current threads have been created, haven't exited, but may not have
    // run yet.
    int curThreads{0};
    int wantThreads{0};

    ITaskNotify * first{nullptr};
    ITaskNotify * last{nullptr};

    condition_variable cv;

    void Push (ITaskNotify & task);
    void Pop ();
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class EndThreadTask : public ITaskNotify {};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<HTaskQueue, TaskQueue> s_queues;
static int s_numThreads;
static mutex s_mut;
static condition_variable s_destroyed;
static int s_numDestroyed;
static int s_numEnded;

static HTaskQueue s_eventQ;
static HTaskQueue s_computeQ;
static atomic_bool s_running;


/****************************************************************************
*
*   Run tasks
*
***/

//===========================================================================
static void TaskQueueThread (TaskQueue * ptr) {
    TaskQueue & q{*ptr};
    bool more{true};
    unique_lock<mutex> lk{s_mut};
    while (more) {
        while (!q.first) 
            q.cv.wait(lk);

        auto * task = q.first;
        q.Pop();
        more = !dynamic_cast<EndThreadTask *>(task);
        lk.unlock();
        task->OnTask();
        lk.lock();
    }
    q.curThreads -= 1;
    s_numThreads -= 1;

    if (!s_numThreads) {
        s_numDestroyed += 1;
        s_destroyed.notify_one();
        lk.unlock();
    }
}

//===========================================================================
static void SetThreads_Lock (TaskQueue & q, int threads) {
    q.wantThreads = threads;
    int num = q.wantThreads - q.curThreads;
    if (num > 0) {
        q.curThreads = q.wantThreads;
        s_numThreads += num;
    }

    if (num > 0) {
        for (int i = 0; i < num; ++i) {
            thread thr{TaskQueueThread, &q};
            thr.detach();
        }
    } else if (num < 0) {
        for (int i = 0; i > num; --i) {
            s_numEnded += 1;
            auto * task = new EndThreadTask;
            q.Push(*task);
        }
        q.cv.notify_all();
    }
}


/****************************************************************************
*
*   TaskQueue
*
***/

//===========================================================================
void TaskQueue::Push (ITaskNotify & task) {
    task.m_taskNext = nullptr;

    if (!first) {
        first = &task;
    } else {
        last->m_taskNext = &task;
    }
    last = &task;
}

//===========================================================================
void TaskQueue::Pop () {
    first = first->m_taskNext;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void ITaskInitialize () {
    s_running = true;
    s_eventQ = TaskCreateQueue("Event", 1);
    s_computeQ = TaskCreateQueue("Compute", 5);
}

//===========================================================================
void ITaskDestroy () {
    s_running = false;
    unique_lock<mutex> lk{s_mut};

    // send shutdown task to all task threads
    for (auto&& q : s_queues)
        SetThreads_Lock(*q.second, 0);

    // wait for all threads to stop
    while (s_numThreads)
        s_destroyed.wait(lk);

    // delete task queues
    for (auto&& q : s_queues)
        s_queues.Erase(q.first);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void TaskPushEvent (ITaskNotify & task) {
    TaskPush(s_eventQ, task);
}

//===========================================================================
void TaskPushCompute (ITaskNotify & task) {
    TaskPush(s_computeQ, task);
}

//===========================================================================
HTaskQueue TaskCreateQueue (const string & name, unsigned threads) {
    assert(s_running);
    assert(threads);
    auto * q = new TaskQueue;
    q->name = name;
    q->wantThreads = 0;
    q->curThreads = 0;

    lock_guard<mutex> lk(s_mut);
    q->hq = s_queues.Insert(q);
    SetThreads_Lock(*q, threads);
    return q->hq;
}

//===========================================================================
void TaskSetQueueThreads (HTaskQueue hq, unsigned threads) {
    assert(s_running || !threads);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.Find(hq);
    SetThreads_Lock(*q, threads);
}

//===========================================================================
void TaskPush (HTaskQueue hq, ITaskNotify & task) {
    assert(s_running);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.Find(hq);
    q->Push(task);
    q->cv.notify_one();
}
