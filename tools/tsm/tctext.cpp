// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tctext.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CmdOpts {
    Path tslfile;
    Path ofile;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static DbProgressInfo s_progress;


/****************************************************************************
*
*   Command line
*
***/

static bool textCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("text")
        .desc("Translate write ahead log (wal) file to human readable text.")
        .action(textCmd);
    cli.opt(&tslfile, "[wal file]")
        .desc("Wal file to dump, extension defaults to '.tsl'");
    cli.opt(&ofile, "[output file]")
        .desc("Output defaults to '<dat file>.txt', '-' for stdout");
}


/****************************************************************************
*
*   Text command
*
***/

//===========================================================================
static bool textCmd(Cli & cli) {
    if (!s_opts.tslfile)
        return cli.badUsage("No value given for <wal file[.tsl]>");
    s_opts.tslfile.defaultExt("tsl");

    ostream * os{nullptr};
    ofstream ofile;
    if (!s_opts.ofile)
        s_opts.ofile.assign(s_opts.tslfile).setExt("txt");
    if (s_opts.ofile == string_view("-")) {
        os = &cout;
    } else {
        ofile.open(s_opts.ofile.str(), ios::trunc);
        if (!ofile) {
            return cli.fail(
                EX_DATAERR,
                string(s_opts.ofile) + ": invalid <outputFile[.txt]>"
            );
        }
        os = &ofile;
    }

    logMsgInfo() << "Dumping " << s_opts.tslfile << " to " << s_opts.ofile;
    tcLogStart();
    auto h = dbOpen(s_opts.tslfile);
    dbClose(h);
    tcLogShutdown(&s_progress);

    return true;
}
