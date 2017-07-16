// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dump.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
namespace fs = std::experimental::filesystem;


/****************************************************************************
*
*   Dump command line
*
***/

static bool dumpCmd(Cli & cli);

static Cli s_cli = Cli{}.command("dump")
    .desc("Create metrics dump file from database.")
    .action(dumpCmd);
static auto & s_dat = s_cli.opt<Path>("[dat file]")
    .desc("Database to dump");
static auto & s_out = s_cli.opt<Path>("[output file]", "")
    .desc("Output defaults to '<dat file>.txt', '-' for stdout");


/****************************************************************************
*     
*   Dump command
*     
***/

//===========================================================================
static bool dumpCmd(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");
    // TODO: add default .dat extension
    logMsgDebug() << "Dumping " << *s_dat;

    auto h = tsdOpen(s_dat->defaultExt("dat").view());
    ostream * os{nullptr};
    ofstream ofile;
    if (!s_out)
        s_out->assign(*s_dat).setExt("txt");
    if (*s_out == string_view("-")) {
        os = &cout;
    } else {
        ofile.open(*s_out, ios::trunc);
        if (!ofile) {
            return cli.fail(
                EX_DATAERR, 
                string(*s_out) + ": invalid <outputFile[.txt]>"
            );
        }
        os = &ofile;
    }
    tsdWriteDump(*os, h);
    tsdClose(h);
    
    appSignalShutdown(EX_OK);
    return true;
}
