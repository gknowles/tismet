// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tscarbon.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Variables
*
***/

static SockMgrHandle s_mgr;
static auto & s_perfTasks = uperf("db.update tasks");

static mutex s_mut;
static condition_variable s_cv;


/****************************************************************************
*
*   CarbonTask
*
***/

namespace {

class CarbonTask : public ITaskNotify {
public:
    CarbonTask(string_view name, TimePoint time, double value);
    ~CarbonTask();

    // Inherited via ITaskNotify
    void onTask() override;

private:
    unique_ptr<char[]> m_name;
    TimePoint m_time;
    double m_value;
};

} // namespace


//===========================================================================
CarbonTask::CarbonTask(string_view name, TimePoint time, double value)
    : m_name{strDup(name)}
    , m_time{time}
    , m_value{value}
{
    s_perfTasks += 1;
}

//===========================================================================
CarbonTask::~CarbonTask() {
    {
        scoped_lock<mutex> lk{s_mut};
        s_perfTasks -= 1;
    }
    s_cv.notify_one();
}

//===========================================================================
void CarbonTask::onTask() {
    auto f = tsDataHandle();
    auto ctx = tsDataOpenContext();
    uint32_t id;
    if (tsDataInsertMetric(&id, f, m_name.get()))
        dbUpdateSample(f, id, m_time, m_value);
    dbCloseContext(ctx);
    delete this;
}


/****************************************************************************
*
*   CarbonConn
*
***/

namespace {

class CarbonConn : public ICarbonSocketNotify, public ITaskNotify {
    string m_buf;
public:
    // Inherited via ICarbonSocketNotify
    void onCarbonValue(
        string_view name,
        TimePoint time,
        double value,
        uint32_t idHint
    ) override;
};

} // namespace

//===========================================================================
void CarbonConn::onCarbonValue(
    string_view name,
    TimePoint time,
    double value,
    uint32_t id
) {
    auto task = new CarbonTask(name, time, value);
    taskPushCompute(task);

    unique_lock<mutex> lk{s_mut};
    while (s_perfTasks >= 100)
        s_cv.wait(lk);
}


/****************************************************************************
*
*   Shutdown monitor
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    if (s_perfTasks > 0)
        shutdownIncomplete();
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsCarbonInitialize() {
    shutdownMonitor(&s_cleanup);
    carbonInitialize();
    s_mgr = sockMgrListen(
        "carbon",
        getFactory<IAppSocketNotify, CarbonConn>(),
        (AppSocket::Family) TismetSocket::kCarbon
    );
}
