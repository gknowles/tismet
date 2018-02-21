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

const Duration kSampleInterval = 1min;


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
    auto now = Clock::now();
    auto f = tsDataHandle();
    auto ctx = tsDataOpenContext();
    perfGetValues(&m_vals);
    for (auto && val : m_vals) {
        auto name = (char *) alloca(val.name.size() + size(s_prefix));
        memcpy(name, s_prefix, size(s_prefix) - 1);
        auto optr = name + size(s_prefix) - 1;
        for (auto && ch : val.name) {
            if (strchr(s_allowedChars, ch)) {
                *optr++ = ch;
            } else {
                if (optr[-1] != '_')
                    *optr++ = '_';
            }
        }
        while (optr[-1] == '_')
            optr -= 1;
        *optr = 0;
        uint32_t id;
        if (!tsDataInsertMetric(&id, f, name))
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
    now = Clock::now();
    auto wait = ceil<minutes>(now) - now;
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
    timerUpdate(&s_sampleTimer, 0ms);
}
