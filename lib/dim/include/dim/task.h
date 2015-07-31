// task.h - dim core
#ifndef DIM_TASK_INCLUDED
#define DIM_TASK_INCLUDED

#include "dim/config.h"

#include "dim/handle.h"

#include <string>

struct HTaskQueue : HandleBase {};

class ITaskNotify {
public:
    virtual ~ITaskNotify () {}
    virtual void OnTask () { delete this; }

private:
    friend class TaskQueue;
    ITaskNotify * m_taskNext = nullptr;
};

void TaskPushEvent (ITaskNotify & task);
void TaskPushCompute (ITaskNotify & task);

HTaskQueue TaskCreateQueue (const std::string & name, unsigned threads);
void TaskSetQueueThreads (HTaskQueue q, unsigned threads);
void TaskPush (HTaskQueue q, ITaskNotify & task);

#endif
