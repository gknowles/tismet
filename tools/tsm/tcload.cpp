// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcload.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Command line
*
***/

static bool loadCmd(Cli & cli);

static Cli s_cli = Cli{}.command("load")
    .desc("Load metrics from dump file into database")
    .action(loadCmd);
static auto & s_dat = s_cli.opt<Path>("[dat file]")
    .desc("Target database");
static auto & s_in = s_cli.opt<Path>("[input file]")
    .desc("File to load (default extension: .txt)");
static auto & s_truncate = s_cli.opt<bool>("truncate", false)
    .desc("Completely replace database contents");


/****************************************************************************
*
*   Variables
*
***/

static TimePoint s_startTime;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void logStart(string_view target, string_view source) {
    s_startTime = Clock::now();
    logMsgInfo() << "Loading " << source << " into " << target;
}

//===========================================================================
static void logShutdown(const DbProgressInfo & info) {
    TimePoint finish = Clock::now();
    std::chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done"
        << "; metrics: " << info.metrics
        << "; samples: " << info.samples
        << "; bytes: " << info.bytes
        << "; seconds: " << elapsed.count();
}


/****************************************************************************
*
*   LoadProgress
*
***/

namespace {

struct LoadProgress : IDbProgressNotify {
    DbHandle m_f;
    DbProgressInfo m_info;

    // Inherited via IDbProgressNotify
    bool OnDbProgress(bool complete, const DbProgressInfo & info) override;
};

} // namespace

static LoadProgress s_progress;

//===========================================================================
bool LoadProgress::OnDbProgress(
    bool complete,
    const DbProgressInfo & info
) {
    if (complete) {
        m_info = info;
        dbClose(m_f);
        m_f = {};
        if (logGetMsgCount(kLogTypeError)) {
            appSignalShutdown(EX_DATAERR);
        } else {
            logShutdown(m_info);
            appSignalShutdown();
        }
    }
    return !appStopping();
}


/****************************************************************************
*
*   ShutdownNotify
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
    if (s_progress.m_f)
        shutdownIncomplete();
}


/****************************************************************************
*
*   Load command
*
***/

//===========================================================================
static bool loadCmd(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");

    s_dat->defaultExt("dat");
    s_in->defaultExt("txt");

    logStart(*s_dat, *s_in);
    if (s_truncate)
        fileRemove(*s_dat);
    auto h = dbOpen(*s_dat);
    if (!h)
        return cli.fail(EX_ABORTED, "Canceled");
    DbConfig conf = {};
    conf.checkpointMaxData = 1'000'000'000;
    conf.checkpointMaxInterval = 24h;
    conf.pageMaxAge = 1min;
    conf.pageScanInterval = 1min;
    dbConfigure(h, conf);
    s_progress.m_f = h;
    dbLoadDump(&s_progress, h, *s_in);

    return cli.fail(EX_PENDING, "");
}
