// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tsperf.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

using SampleInterval = minutes; // duration<int, ratio<10>>; // minutes;


/****************************************************************************
*
*   PerfRecordTimer
*
***/

namespace {

class SampleTimer : public ITimerNotify, ITaskNotify {
    Duration onTimer(TimePoint now) override;
    void onTask() override;

    vector<PerfValue> m_vals;
    string m_tmp;
};

} // namespace

static SampleTimer s_sampleTimer;
static atomic_bool s_taskQueued{false};

const char s_prefix[] = "tismet.";
const char s_allowedChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-.";

//===========================================================================
Duration SampleTimer::onTimer(TimePoint now) {
    if (!appStopping()) {
        s_taskQueued = true;
        taskPushCompute(this);
    }

    return kTimerInfinite;
}

//===========================================================================
void SampleTimer::onTask() {
    auto now = timeNow();
    auto f = tsDataHandle();
    auto ctx = tsDataOpenContext();
    perfGetValues(&m_vals);
    for (auto && val : m_vals) {
        m_tmp.reserve(val.name.size() + size(s_prefix));
        m_tmp.assign(s_prefix);
        for (auto && ch : val.name) {
            if (strchr(s_allowedChars, ch)) {
                m_tmp.push_back(ch);
            } else {
                if (m_tmp.back() != '_')
                    m_tmp.push_back('_');
            }
        }
        while (m_tmp.back() == '_')
            m_tmp.pop_back();
        uint32_t id;
        if (!tsDataInsertMetric(&id, f, m_tmp))
            continue;
        DbMetricInfo info = {};
        switch (val.type) {
        case PerfType::kFloat: info.type = kSampleTypeFloat32; break;
        case PerfType::kInt: info.type = kSampleTypeInt32; break;
        case PerfType::kUnsigned: info.type = kSampleTypeFloat64; break;
        default:
            assert(!"unknown perf type");
            break;
        }
        dbUpdateMetric(f, id, info);
        dbUpdateSample(f, id, now, val.raw);
    }
    dbCloseContext(ctx);
    now = timeNow();
    auto wait = ceil<SampleInterval>(now) - now;
    timerUpdate(this, wait);
    s_taskQueued = false;
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
    if (s_taskQueued)
        shutdownIncomplete();
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsPerfInitialize() {
    shutdownMonitor(&s_cleanup);
    timerUpdate(&s_sampleTimer, 0ms);
}
