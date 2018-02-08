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

class SampleTimer : public ITimerNotify {
    Duration onTimer(TimePoint now) override;

    vector<PerfValue> m_vals;
};

} // namespace

const char s_prefix[] = "tismet.";
const char s_allowedChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-.";

//===========================================================================
Duration SampleTimer::onTimer(TimePoint now) {
    if (appStopping())
        return kTimerInfinite;

    auto ctx = tsDataOpenContext();
    perfGetValues(&m_vals);
    for (auto && val : m_vals) {
        if (val.name.substr(0, 3) != "db ")
            continue;
        auto rname = val.name.substr(3);
        auto name = (char *) alloca(rname.size() + size(s_prefix));
        memcpy(name, s_prefix, size(s_prefix) - 1);
        auto optr = name + size(s_prefix) - 1;
        for (auto && ch : val.name.substr(3)) {
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
        tsDataInsertMetric(&id, ctx, name);
        MetricInfo info = {};
        switch (val.type) {
        case PerfType::kFloat: info.type = kSampleTypeFloat32; break;
        case PerfType::kInt: info.type = kSampleTypeInt32; break;
        case PerfType::kUnsigned: info.type = kSampleTypeFloat64; break;
        default:
            assert(!"unknown perf type");
            break;
        }
        dbUpdateMetric(ctx, id, info);
        dbUpdateSample(ctx, id, now, val.raw);
    }
    dbCloseContext(ctx);
    now = Clock::now();
    auto wait = ceil<minutes>(now) - now;
    return wait;
}


/****************************************************************************
*
*   Public API
*
***/

static SampleTimer s_sampleTimer;

//===========================================================================
void tsPerfInitialize() {
    timerUpdate(&s_sampleTimer, 0ms);
}
