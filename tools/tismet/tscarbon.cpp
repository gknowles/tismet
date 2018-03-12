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


/****************************************************************************
*
*   CarbonTask
*
***/

namespace {

class CarbonTask : public ITaskNotify {
public:
    CarbonTask(unsigned reqId, string_view name, TimePoint time, double value);
    ~CarbonTask();

    // Inherited via ITaskNotify
    void onTask() override;

private:
    unsigned m_reqId;
    unique_ptr<char[]> m_name;
    TimePoint m_time;
    double m_value;
};

} // namespace


//===========================================================================
CarbonTask::CarbonTask(
    unsigned reqId,
    string_view name,
    TimePoint time,
    double value
)
    : m_reqId{reqId}
    , m_name{strDup(name)}
    , m_time{time}
    , m_value{value}
{
    s_perfTasks += 1;
}

//===========================================================================
CarbonTask::~CarbonTask() {
    s_perfTasks -= 1;
}

//===========================================================================
void CarbonTask::onTask() {
    if (auto name = m_name.get()) {
        auto f = tsDataHandle();
        auto ctx = tsDataOpenContext();
        uint32_t id;
        if (tsDataInsertMetric(&id, f, name))
            dbUpdateSample(f, id, m_time, m_value);
        dbCloseContext(ctx);
        m_name.reset();
        taskPushEvent(this);
        return;
    }

    carbonAckValue(m_reqId, 1);
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
    bool onCarbonValue(
        unsigned reqId,
        string_view name,
        TimePoint time,
        double value,
        uint32_t idHint
    ) override;
};

} // namespace

//===========================================================================
bool CarbonConn::onCarbonValue(
    unsigned reqId,
    string_view name,
    TimePoint time,
    double value,
    uint32_t idHint
) {
    auto task = new CarbonTask(reqId, name, time, value);
    taskPushCompute(task);
    return false;
}


/****************************************************************************
*
*   Shutdown monitor
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownClient(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownClient(bool firstTry) {
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
