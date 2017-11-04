// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcdump.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Command line
*
***/

static bool dumpCmd(Cli & cli);

static Cli s_cli = Cli{}.command("dump")
    .desc("Create dump file from metrics database.")
    .action(dumpCmd);
static auto & s_dat = s_cli.opt<Path>("[dat file]")
    .desc("Database to dump");
static auto & s_out = s_cli.opt<Path>("[output file]")
    .desc("Output defaults to '<dat file>.txt', '-' for stdout");
static auto & s_qry = s_cli.opt<string>("f find")
    .desc("Wildcard metric name to match, defaults to matching all metrics.");


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
    logMsgInfo() << "Dumping " << source << " into " << target;
}

//===========================================================================
static void logShutdown(const TsdProgressInfo & info) {
    TimePoint finish = Clock::now();
    std::chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done"
        << "; metrics: " << info.metrics
        << "; values: " << info.values
        << "; bytes: " << info.bytes
        << "; seconds: " << elapsed.count();
}


/****************************************************************************
*     
*   LoadProgress
*     
***/

namespace {

struct LoadProgress : ITsdProgressNotify {
    TsdProgressInfo m_info;

    // Inherited via ITsdProgressNotify
    bool OnTsdProgress(bool complete, const TsdProgressInfo & info) override;
};

} // namespace

//===========================================================================
bool LoadProgress::OnTsdProgress(
    bool complete, 
    const TsdProgressInfo & info
) {
    if (complete) 
        m_info = info;
    return true;
}


/****************************************************************************
*     
*   Dump command
*     
***/

//===========================================================================
static bool dumpCmd(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");
    s_dat->defaultExt("dat");

    ostream * os{nullptr};
    ofstream ofile;
    if (!s_out)
        s_out->assign(*s_dat).setExt("txt");
    if (*s_out == string_view("-")) {
        os = &cout;
    } else {
        ofile.open(s_out->str(), ios::trunc);
        if (!ofile) {
            return cli.fail(
                EX_DATAERR, 
                string(s_out->c_str()) + ": invalid <outputFile[.txt]>"
            );
        }
        os = &ofile;
    }

    logStart(*s_out, *s_dat);
    auto h = tsdOpen(*s_dat);
    LoadProgress progress;
    tsdWriteDump(&progress, *os, h, *s_qry);
    tsdClose(h);
    logShutdown(progress.m_info);

    return true;
}
