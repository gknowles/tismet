// Copyright Glen Knowles 2017 - 2018.
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
*   LoadProgress
*
***/

namespace {

struct LoadProgress : IDbProgressNotify {
    DbProgressInfo m_info;

    // Inherited via IDbProgressNotify
    bool onDbProgress(RunMode mode, const DbProgressInfo & info) override;
};

} // namespace

//===========================================================================
bool LoadProgress::onDbProgress(
    RunMode mode,
    const DbProgressInfo & info
) {
    if (mode == kRunStopped)
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

    logMsgInfo() << "Dumping " << *s_out << " to " << *s_dat;
    tcLogStart();
    auto h = dbOpen(*s_dat);
    LoadProgress progress;
    dbWriteDump(&progress, *os, h, *s_qry);
    dbClose(h);
    tcLogShutdown(&progress.m_info);

    return true;
}
