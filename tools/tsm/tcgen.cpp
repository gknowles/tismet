// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcgen.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
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

    TimePoint startTime;
    TimePoint endTime;

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

static CmdOpts s_opts;

static FileHandle s_file;
static uint64_t s_bytesWritten;
static uint64_t s_valuesWritten;

static string s_buffer;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void flushValues() {
    if (s_buffer.empty())
        return;
    fileAppendWait(s_file, s_buffer.data(), s_buffer.size());
    s_buffer.clear();
}

//===========================================================================
static bool writeValue(const Metric & met) {
    auto base = s_buffer.size();
    carbonWrite(s_buffer, met.name, met.time, (float) met.value);
    auto len = base - s_buffer.size();
    if (s_buffer.size() > 4096) {
        flushValues();
    }
    s_bytesWritten += len;
    s_valuesWritten += 1;
    if (s_opts.maxBytes && s_bytesWritten >= s_opts.maxBytes
        || s_opts.maxValues && s_valuesWritten >= s_opts.maxValues
    ) {
        return false;
    }
    return true;
}

//===========================================================================
static bool advanceValue(
    Metric & met, 
    default_random_engine & reng,
    uniform_real_distribution<> & rdist
) {
    if (s_opts.endTime.time_since_epoch().count() 
        && met.time >= s_opts.endTime
    ) {
        return false;
    }
    met.time += (seconds) s_opts.intervalSecs;
    met.value += rdist(reng);
    return true;
}

//===========================================================================
static void genValues() {
    // create metrics
    static const char * numerals[] = {
        "zero.", "one.", "two.", "three.", "four.",
        "five.", "six.", "seven.", "eight.", "nine.",
    };
    vector<Metric> metrics(s_opts.metrics);
    IntegralStr<unsigned> str{0};
    for (unsigned i = 0; i < s_opts.metrics; ++i) {
        auto & met = metrics[i];
        for (auto && ch : str.set(i)) {
            met.name += numerals[ch - '0'];
            met.value = 0;
            met.time = s_opts.startTime;
        }
        // remove extra trailing dot
        met.name.pop_back();
    }

    random_device rdev;
    default_random_engine reng(rdev());
    uniform_real_distribution<> rdist(s_opts.minDelta, s_opts.maxDelta);

    for (;;) {
        for (auto && met : metrics) {
            if (!writeValue(met))
                return;
            if (!advanceValue(met, reng, rdist))
                return;
        }
    }
}

//===========================================================================
static void genValuesThread() {
    genValues();
    flushValues();
    fileClose(s_file);
    appSignalShutdown();
}


/****************************************************************************
*
*   Command line
*
***/

static bool genCmd(Cli & cli);

// 1998-07-09 16:00:00 UTC
constexpr TimePoint kDefaultStartTime{900'000'000s}; 

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("gen")
        .desc("Generate test metrics.")
        .action(genCmd)
        .group("Target").sortKey("1");
    cli.opt<Path>(&ofile, "F file")
        .desc("Output file, '-' for stdout, extension defaults to '.txt'")
        .require();

    cli.group("When to Stop").sortKey("2");
    cli.opt(&maxBytes, "B bytes", 0)
        .desc("Max bytes to generate, 0 for unlimited");
    cli.opt(&maxSecs, "S seconds", 0)
        .desc("Max seconds to run, 0 for unlimited");
    cli.opt(&maxValues, "V values", 10)
        .desc("Max values to generate, 0 for unlimited");

    cli.group("Metrics to Generate").sortKey("3");
    cli.opt(&metrics, "m metrics", 100)
        .desc("Number of metrics");
    cli.opt(&startTime, "s start", kDefaultStartTime)
        .desc("Start time of first metric value")
        .valueDesc("TIME");
    cli.opt(&endTime, "e end")
        .desc("Time of last metric value, rounded up to next interval")
        .valueDesc("TIME");
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
