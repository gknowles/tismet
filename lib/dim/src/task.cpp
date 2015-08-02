// task.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::rel_ops;


/****************************************************************************
*
*   Incomplete public types
*
***/

class DimTaskQueue {
public:
    HDimTaskQueue hq;
    string name;

    // current threads have been created, haven't exited, but may not have
    // run yet.
    int curThreads{0};
    int wantThreads{0};

    IDimTaskNotify * first{nullptr};
    IDimTaskNotify * last{nullptr};

    condition_variable cv;

    void Push (IDimTaskNotify & task);
    void Pop ();
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class EndThreadTask : public IDimTaskNotify {};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static DimHandleMap<HDimTaskQueue, DimTaskQueue> s_queues;
static int s_numThreads;
static mutex s_mut;
static condition_variable s_destroyed;
static int s_numDestroyed;
static int s_numEnded;

static HDimTaskQueue s_eventQ;
static HDimTaskQueue s_computeQ;
static atomic_bool s_running;


/****************************************************************************
*
*   Run tasks
*
***/

//===========================================================================
static void TaskQueueThread (DimTaskQueue * ptr) {
    DimTaskQueue & q{*ptr};
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
static void SetThreads_Lock (DimTaskQueue & q, int threads) {
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
*   DimTaskQueue
*
***/

//===========================================================================
void DimTaskQueue::Push (IDimTaskNotify & task) {
    task.m_taskNext = nullptr;

    if (!first) {
        first = &task;
    } else {
        last->m_taskNext = &task;
    }
    last = &task;
}

//===========================================================================
void DimTaskQueue::Pop () {
    first = first->m_taskNext;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimTaskInitialize () {
    s_running = true;
    s_eventQ = DimTaskCreateQueue("Event", 1);
    s_computeQ = DimTaskCreateQueue("Compute", 5);
}

//===========================================================================
void IDimTaskDestroy () {
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
void DimTaskPushEvent (IDimTaskNotify & task) {
    DimTaskPush(s_eventQ, task);
}

//===========================================================================
void DimTaskPushCompute (IDimTaskNotify & task) {
    DimTaskPush(s_computeQ, task);
}

//===========================================================================
HDimTaskQueue DimTaskCreateQueue (const string & name, unsigned threads) {
    assert(s_running);
    assert(threads);
    auto * q = new DimTaskQueue;
    q->name = name;
    q->wantThreads = 0;
    q->curThreads = 0;

    lock_guard<mutex> lk(s_mut);
    q->hq = s_queues.Insert(q);
    SetThreads_Lock(*q, threads);
    return q->hq;
}

//===========================================================================
void DimTaskSetQueueThreads (HDimTaskQueue hq, unsigned threads) {
    assert(s_running || !threads);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.Find(hq);
    SetThreads_Lock(*q, threads);
}

//===========================================================================
void DimTaskPush (HDimTaskQueue hq, IDimTaskNotify & task) {
    assert(s_running);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.Find(hq);
    q->Push(task);
    q->cv.notify_one();
}
