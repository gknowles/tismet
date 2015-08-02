// task.h - dim core
#ifndef DIM_TASK_INCLUDED
#define DIM_TASK_INCLUDED

#include "dim/config.h"

#include "dim/handle.h"

#include <string>

struct HDimTaskQueue : DimHandleBase {};

class IDimTaskNotify {
public:
    virtual ~IDimTaskNotify () {}
    virtual void OnTask () { delete this; }

private:
    friend class DimTaskQueue;
    IDimTaskNotify * m_taskNext = nullptr;
};

void DimTaskPushEvent (IDimTaskNotify & task);
void DimTaskPushCompute (IDimTaskNotify & task);

HDimTaskQueue DimTaskCreateQueue (const std::string & name, unsigned threads);
void DimTaskSetQueueThreads (HDimTaskQueue q, unsigned threads);
void DimTaskPush (HDimTaskQueue q, IDimTaskNotify & task);

#endif
