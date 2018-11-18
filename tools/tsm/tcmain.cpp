// Copyright Glen Knowles 2015 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tcmain.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

char const kVersion[] = "1.0.0";


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
    auto version = string(kVersion) + " (" __DATE__ ")";
    cli.header("tsm v"s + version);
    cli.versionOpt(version, "tsm");
    cli.desc("Utility for dealing with metrics and the tismet server.");
    cli.helpCmd().helpNoArgs();
    (void) cli.exec(argc, argv);
    return appSignalUsageError();
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

    int code = appRun(app, argc, argv);
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
    DbProgressInfo const & info,
    chrono::duration<double> time
) {
    if (auto num = info.files)
        os << "; files: " << num;
    if (auto num = info.metrics)
        os << "; metrics: " << num;
    if (auto num = info.samples)
        os << "; samples: " << num;
    if (auto num = info.bytes)
        os << "; bytes: " << num;
    if (auto num = time.count())
        os << "; seconds: " << num;
}

//===========================================================================
static void dumpTotals(
    ostream & os,
    DbProgressInfo const & info,
    chrono::duration<double> time
) {
    if (auto num = info.totalFiles; num && num != (size_t) -1)
        os << "; files: " << num;
    if (auto num = info.totalMetrics; num && num != (size_t) -1)
        os << "; metrics: " << num;
    if (auto num = info.totalSamples; num && num != (size_t) -1)
        os << "; samples: " << num;
    if (auto num = info.totalBytes; num && num != (size_t) -1)
        os << "; bytes: " << num;
    if (auto num = time.count())
        os << "; seconds: " << num;
}

//===========================================================================
void tcLogStart(
    DbProgressInfo const * limit,
    chrono::duration<double> timeLimit
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
void tcLogShutdown(DbProgressInfo const * total) {
    TimePoint finish = timeNow();
    chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done";
    if (total) {
        dump(os, *total, elapsed);
    } else {
        dump(os, {}, elapsed);
    }
}
