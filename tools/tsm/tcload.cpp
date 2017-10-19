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
static void logShutdown(const TsdProgressInfo & info) {
    TimePoint finish = Clock::now();
    std::chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done; values: " << info.values
        << "; bytes: " << info.bytes
        << "; seconds: " << elapsed.count();
}


/****************************************************************************
*     
*   LoadProgress
*     
***/

namespace {

class LoadProgress : public ITsdProgressNotify {
public:
    LoadProgress(TsdFileHandle h) : m_h{h} {}

    // Inherited via ITsdProgressNotify
    bool OnTsdProgress(bool complete, const TsdProgressInfo & info) override;

private:
    TsdFileHandle m_h;
};

} // namespace

//===========================================================================
bool LoadProgress::OnTsdProgress(
    bool complete, 
    const TsdProgressInfo & info
) {
    if (complete) {
        tsdClose(m_h);
        logShutdown(info);
        appSignalShutdown(EX_OK);
        delete this;
    }
    return true;
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
    auto h = tsdOpen(*s_dat);
    auto progress = make_unique<LoadProgress>(h);
    tsdLoadDump(progress.release(), h, *s_in);

    return cli.fail(EX_PENDING, "");
}
