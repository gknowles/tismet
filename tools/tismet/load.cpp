// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// load.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*     
*   Load command
*     
***/

static bool run(Cli & cli);

static Cli s_cli = Cli{}.command("load")
    .desc("Load metrics dump file into database")
    .action(run);
static auto & s_dat = s_cli.opt<string>("[dat file]");


/****************************************************************************
*     
*   Load action
*     
***/

//===========================================================================
static bool run(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");

    auto h = tsdOpen(*s_dat);
    tsdClose(h);

    return true;
}
