// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dump.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Dump command line
*
***/

static bool run(Cli & cli);

static Cli s_cli = Cli{}.command("dump")
    .desc("Create database dump that can be loaded into another database")
    .action(run);
static auto & s_dat = s_cli.opt<string>("[dat file]");


/****************************************************************************
*     
*   Dump action
*     
***/

//===========================================================================
static bool run(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");
        
    cout << "Dumping " << *s_dat << endl;

    auto h = tsdOpen(*s_dat);
    tsdDump(cout, h);
    tsdClose(h);

    return true;
}
