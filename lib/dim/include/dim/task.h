// task.h - dim core
#pragma once

#include "dim/config.h"
#include "dim/handle.h"

#include <string>

namespace Dim {

/****************************************************************************
*
*   Task queue
*
***/

struct TaskQueueHandle : HandleBase {};

class ITaskNotify {
public:
    virtual ~ITaskNotify () {}
    virtual void onTask () { delete this; }

private:
    friend class TaskQueue;
    ITaskNotify * m_taskNext = nullptr;
};

void taskPushEvent (ITaskNotify & task);
void taskPushEvent (ITaskNotify * tasks[], size_t numTasks);

void taskPushCompute (ITaskNotify & task);
void taskPushCompute (ITaskNotify * tasks[], size_t numTasks);

TaskQueueHandle taskCreateQueue (const std::string & name, int threads);
void taskSetQueueThreads (TaskQueueHandle q, int threads);
void taskPush (TaskQueueHandle q, ITaskNotify & task);
void taskPush (TaskQueueHandle q, ITaskNotify * tasks[], size_t numTasks);

} // namespace
