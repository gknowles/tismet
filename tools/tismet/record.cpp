// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// record.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Record command line
*
***/

static bool recordCmd(Cli & cli);

static Cli s_cli = Cli{}.command("record")
    .desc("Create recording of metrics received via carbon protocol.")
    .action(recordCmd);
static auto & s_out = s_cli.opt<Path>("<output file>", "")
    .desc("Extension defaults to '.txt', '-' for stdout");
static auto & s_endpt = s_cli.opt<Path>("<endpoint>");


/****************************************************************************
*     
*   Record command
*     
***/

//===========================================================================
static bool recordCmd(Cli & cli) {
    if (!s_out)
        return cli.badUsage("No value given for <output file[.txt]>");
    // TODO: add default .dat extension
    logMsgDebug() << "Recording to " << *s_out;
    logMsgDebug() << "Control-C to stop recording";
    consoleEnableCtrlC();

    ostream * os{nullptr};
    ofstream ofile;
    if (*s_out == string_view("-")) {
        os = &cout;
    } else {
        ofile.open(s_out->defaultExt("txt"), ios::trunc);
        if (!ofile) {
            return cli.fail(
                EX_DATAERR, 
                string(*s_out) + ": invalid <outputFile[.txt]>"
            );
        }
        os = &ofile;
    }
    
    carbonInitialize();
    return true;
}
