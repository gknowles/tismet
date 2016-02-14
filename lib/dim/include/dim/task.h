// task.h - dim core
#ifndef DIM_TASK_INCLUDED
#define DIM_TASK_INCLUDED

#include "dim/config.h"

#include "dim/handle.h"

#include <string>


/****************************************************************************
*
*   Task queue
*
***/

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
void DimTaskPushEvent (IDimTaskNotify * tasks[], size_t numTasks);

void DimTaskPushCompute (IDimTaskNotify & task);
void DimTaskPushCompute (IDimTaskNotify * tasks[], size_t numTasks);

HDimTaskQueue DimTaskCreateQueue (const std::string & name, int threads);
void DimTaskSetQueueThreads (HDimTaskQueue q, int threads);
void DimTaskPush (HDimTaskQueue q, IDimTaskNotify & task);
void DimTaskPush (HDimTaskQueue q, IDimTaskNotify * tasks[], size_t numTasks);

#endif
