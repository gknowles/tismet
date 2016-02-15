// task.cpp - dim core
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::rel_ops;

namespace Dim {


/****************************************************************************
*
*   Incomplete public types
*
***/

class TaskQueue {
public:
    TaskQueueHandle hq;
    string name;

    // current threads have been created, haven't exited, but may not have
    // run yet.
    int curThreads{0};
    int wantThreads{0};

    ITaskNotify * first{nullptr};
    ITaskNotify * last{nullptr};

    condition_variable cv;

    void push (ITaskNotify & task);
    void pop ();
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

static HandleMap<TaskQueueHandle, TaskQueue> s_queues;
static int s_numThreads;
static mutex s_mut;
static condition_variable s_destroyed;
static int s_numDestroyed;
static int s_numEnded;

static TaskQueueHandle s_eventQ;
static TaskQueueHandle s_computeQ;
static atomic_bool s_running;


/****************************************************************************
*
*   Run tasks
*
***/

//===========================================================================
static void taskQueueThread (TaskQueue * ptr) {
    TaskQueue & q{*ptr};
    bool more{true};
    unique_lock<mutex> lk{s_mut};
    while (more) {
        while (!q.first) 
            q.cv.wait(lk);

        auto * task = q.first;
        q.pop();
        more = !dynamic_cast<EndThreadTask *>(task);
        lk.unlock();
        task->onTask();
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
static void setThreads_LK (TaskQueue & q, size_t threads) {
    q.wantThreads = (int) threads;
    int num = q.wantThreads - q.curThreads;
    if (num > 0) {
        q.curThreads = q.wantThreads;
        s_numThreads += num;
    }

    if (num > 0) {
        for (int i = 0; i < num; ++i) {
            thread thr{taskQueueThread, &q};
            thr.detach();
        }
    } else if (num < 0) {
        for (int i = 0; i > num; --i) {
            s_numEnded += 1;
            auto * task = new EndThreadTask;
            q.push(*task);
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
void TaskQueue::push (ITaskNotify & task) {
    task.m_taskNext = nullptr;
    if (!first) {
        first = &task;
    } else {
        last->m_taskNext = &task;
    }
    last = &task;
}

//===========================================================================
void TaskQueue::pop () {
    auto * task = first;
    first = task->m_taskNext;
    task->m_taskNext = nullptr;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iTaskInitialize () {
    s_running = true;
    s_eventQ = taskCreateQueue("Event", 1);
    s_computeQ = taskCreateQueue("Compute", 5);
}

//===========================================================================
void iTaskDestroy () {
    s_running = false;
    unique_lock<mutex> lk{s_mut};

    // send shutdown task to all task threads
    for (auto&& q : s_queues)
        setThreads_LK(*q.second, 0);

    // wait for all threads to stop
    while (s_numThreads)
        s_destroyed.wait(lk);

    // delete task queues
    for (auto&& q : s_queues)
        s_queues.erase(q.first);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void taskPushEvent (ITaskNotify & task) {
    ITaskNotify * list[] = { &task };
    taskPushEvent(list, size(list));
}

//===========================================================================
void taskPushEvent (ITaskNotify * tasks[], size_t numTasks) {
    taskPush(s_eventQ, tasks, numTasks);
}

//===========================================================================
void taskPushCompute (ITaskNotify & task) {
    ITaskNotify * list[] = { &task };
    taskPushCompute(list, size(list));
}

//===========================================================================
void taskPushCompute (ITaskNotify * tasks[], size_t numTasks) {
    taskPush(s_computeQ, tasks, numTasks);
}

//===========================================================================
TaskQueueHandle taskCreateQueue (const string & name, int threads) {
    assert(s_running);
    assert(threads);
    auto * q = new TaskQueue;
    q->name = name;
    q->wantThreads = 0;
    q->curThreads = 0;

    lock_guard<mutex> lk(s_mut);
    q->hq = s_queues.insert(q);
    setThreads_LK(*q, threads);
    return q->hq;
}

//===========================================================================
void taskSetQueueThreads (TaskQueueHandle hq, int threads) {
    assert(s_running || !threads);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.find(hq);
    setThreads_LK(*q, threads);
}

//===========================================================================
void taskPush (TaskQueueHandle hq, ITaskNotify & task) {
    ITaskNotify * list[] = { &task };
    taskPush(hq, list, size(list));
}

//===========================================================================
void taskPush (
    TaskQueueHandle hq, 
    ITaskNotify * tasks[], 
    size_t numTasks
) {
    assert(s_running);

    lock_guard<mutex> lk{s_mut};
    auto * q = s_queues.find(hq);
    for (int i = 0; i < numTasks; ++tasks, ++i) 
        q->push(**tasks);

    if (numTasks > 1 && q->curThreads > 1) {
        q->cv.notify_all();
    } else {
        q->cv.notify_one();
    }
}

} // namespace
