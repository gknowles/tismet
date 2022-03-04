// Copyright Glen Knowles 2015 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// tcmain.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const VersionInfo kVersion = { 1, 1 };


/****************************************************************************
*
*   Variables
*
***/

static TimePoint s_startTime;


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    cli.desc("Utility for dealing with metrics and the tismet server.");
    cli.helpCmd().helpNoArgs();
    cli.exec(argc, argv);
    appSignalUsageError();
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF
        | _CRTDBG_LEAK_CHECK_DF
//        | _CRTDBG_DELAY_FREE_MEM_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    int code = appRun(app, argc, argv, kVersion);
    return code;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
static void dump(
    ostream & os,
    size_t files,
    size_t metrics,
    size_t samples,
    size_t bytes,
    Duration time
) {
    pair<const char *, size_t> nums[] = {
        { "files", files },
        { "metrics", metrics },
        { "samples", samples },
        { "bytes", bytes },
    };
    bool found = false;
    for (auto&& num : nums) {
        if (num.second && num.second != (size_t) -1) {
            found = true;
            os << "; " << num.first << ": " << num.second;
        }
    }
    if (time.count()) {
        found = true;
        auto tstr = toString(time, DurationFormat::kTwoPart);
        os << "; time: " << tstr;
    }
    if (!found)
        os << "; none";
}

//===========================================================================
static void dump(
    ostream & os,
    const DbProgressInfo & info,
    Duration time
) {
    dump(os, info.files, info.metrics, info.samples, info.bytes, time);
}

//===========================================================================
static void dumpTotals(
    ostream & os,
    const DbProgressInfo & info,
    Duration time
) {
    dump(
        os,
        info.totalFiles,
        info.totalMetrics,
        info.totalSamples,
        info.totalBytes,
        time
    );
}

//===========================================================================
void tcLogStart(
    const DbProgressInfo * limit,
    Duration timeLimit
) {
    s_startTime = timeNow();
    if (limit
        && (limit->totalFiles
            || limit->totalMetrics
            || limit->totalSamples
            || limit->totalBytes
            || timeLimit.count()
        )
    ) {
        auto os = logMsgInfo();
        os.imbue(locale(""));
        os << "Limits";
        dumpTotals(os, *limit, timeLimit);
    }
}

//===========================================================================
void tcLogShutdown(const DbProgressInfo * total) {
    TimePoint finish = timeNow();
    auto elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done";
    if (total) {
        dump(os, *total, elapsed);
    } else {
        dump(os, {}, elapsed);
    }
}
