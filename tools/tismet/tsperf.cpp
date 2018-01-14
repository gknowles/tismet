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
    unordered_map<string_view, uint32_t> m_idByName;
};

} // namespace

const char s_allowedChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-.";

//===========================================================================
Duration SampleTimer::onTimer(TimePoint now) {
    if (appStopping())
        return kTimerInfinite;

    auto h = tsDataHandle();
    perfGetValues(m_vals);
    for (auto && val : m_vals) {
        if (val.name.substr(0, 3) != "db ")
            continue;
        auto & id = m_idByName[val.name];
        if (!id) {
            string name = "tismet.";
            for (auto && ch : val.name.substr(3)) {
                if (strchr(s_allowedChars, ch)) {
                    name += ch;
                } else {
                    if (name.back() != '_')
                        name += '_';
                }
            }
            if (name.back() == '_')
                name.pop_back();
            dbInsertMetric(id, h, name);
        }
        dbUpdateSample(h, id, now, (float) val.raw);
    }
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
