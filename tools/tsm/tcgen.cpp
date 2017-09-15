// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcgen.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

struct CmdOpts {
    Path ofile;
    uint64_t maxBytes;
    unsigned maxSecs;
    uint64_t maxValues;

    unsigned metrics;
    unsigned intervalSecs;
    double minDelta;
    double maxDelta;

    CmdOpts();
};

struct Metric {
    string name;
    double value;
    TimePoint time;
};


/****************************************************************************
*
*   Variables
*
***/

static FileHandle s_file;
static uint64_t s_bytesWritten;

static CmdOpts s_opts;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void genValuesThread() {
    // create metrics
    vector<Metric> metrics(s_opts.metrics);

    fileClose(s_file);
    appSignalShutdown();
}


/****************************************************************************
*
*   Command line
*
***/

static bool genCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("gen")
        .desc("Generate test metrics.")
        .action(genCmd)
        .group("Target").sortKey("1");
    cli.opt<Path>(&ofile, "F file")
        .desc("Output file, '-' for stdout, extension defaults to '.txt'")
        .valueDesc("FILE");

    cli.group("Quantity").sortKey("2");
    cli.opt(&maxBytes, "B bytes", 0)
        .desc("Max bytes to generate, 0 for unlimited");
    cli.opt(&maxSecs, "S seconds", 0)
        .desc("Max seconds to run, 0 for unlimited");
    cli.opt(&maxValues, "V values", 0)
        .desc("Max values to generate, 0 for unlimited");

    cli.group("Metrics").sortKey("3");
    cli.opt(&metrics, "m metrics", 100)
        .desc("Number of metrics");
    //cli.opt(&startTime, "s start", 
    cli.opt(&intervalSecs, "i interval", 60)
        .desc("Seconds between metric values");
    cli.opt(&minDelta, "dmin", 0.0)
        .desc("Minimum delta between consecutive values")
        .valueDesc("FLOAT");
    cli.opt(&maxDelta, "dmax", 10.0)
        .desc("Max delta between consecutive values")
        .valueDesc("FLOAT");
}

//===========================================================================
static bool genCmd(Cli & cli) {
    auto fname = s_opts.ofile;
    if (!fname)
        return cli.badUsage("No value given for <output file[.txt]>");
    if (fname.view() == "-") {
        s_file = fileAttachStdout();
    } else {
        s_file = fileOpen(
            fname.defaultExt("txt"), 
            File::fReadWrite | File::fCreat | File::fTrunc | File::fBlocking
        );
        if (!s_file) {
            return cli.fail(
                EX_DATAERR, 
                fname.str() + ": open <outputFile[.txt]> failed"
            );
        }
    }

    logMsgInfo() << "Writing to " << fname;
    if (s_opts.maxBytes || s_opts.maxSecs || s_opts.maxValues) {
        auto os = logMsgInfo();
        os << "Limits";
        if (auto num = s_opts.maxValues) 
            os << ", values: " << num;
        if (auto num = s_opts.maxBytes)
            os << ", bytes: " << num;
        if (auto num = s_opts.maxSecs)
            os << ", seconds: " << num;
    }
    consoleEnableCtrlC();

    taskPushOnce("Generate Metrics", genValuesThread);
    return true;
}
