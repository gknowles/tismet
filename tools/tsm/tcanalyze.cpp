// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// tcanalyze.cpp - tsm
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
    Path reportfile;

    Path ofile;
    FileAppendStream::OpenExisting openMode;

    DbProgressInfo progress;

    CmdOpts();
};

struct MetricInfo {
    TimePoint time;
    double value;
    int64_t dt;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static DbProgressInfo s_progress;
static FileAppendStream s_file;

// deltas found as measured in bits
static map<int, unsigned> s_timeDeltas;
static unordered_map<string, MetricInfo> s_metrics;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void writeResults() {
    ostringstream os;
    os << "Distribution of storage requirements for time deltas (bits)\n";
    for (auto && kv : s_timeDeltas)
        os << kv.first << " " << kv.second << '\n';
    if (s_file) {
        s_file.append(os.str());
    } else {
        cout << os.str();
    }
}


/****************************************************************************
*
*   RecordFile
*
***/

namespace {

class RecordFile : public ICarbonFileNotify {
public:
    bool onCarbonValue(
        unsigned reqId,
        string_view name,
        TimePoint time,
        double value,
        uint32_t idHint
    ) override;
private:
    bool onFileRead(
        size_t * bytesUsed,
        string_view data,
        bool more,
        int64_t offset,
        FileHandle f
    ) override;

    string m_buf;
};

} // namespace

static RecordFile s_source;

//===========================================================================
bool RecordFile::onCarbonValue(
    unsigned reqId,
    string_view name,
    TimePoint time,
    double value,
    uint32_t idHint
) {
    if (appStopping())
        return true;
    s_progress.samples += 1;

    m_buf = name;
    auto & samp = s_metrics[m_buf];
    auto dt = (time - samp.time) / 1s;
    auto ddt = dt - samp.dt;
    int dbit;
    if (ddt < 0) {
        dbit = -(63 - countl_zero((uint64_t) -ddt));
    } else if (ddt == 0) {
        dbit = 0;
    } else {
        dbit = 63 - countl_zero((uint64_t) ddt);
    }
    s_timeDeltas[dbit] += 1;
    samp = {time, value, !empty(samp.time) ? dt : 0};

    if (s_opts.progress.totalSamples
            && s_opts.progress.samples == s_opts.progress.totalSamples
    ) {
        appSignalShutdown();
    }
    return true;
}

//===========================================================================
bool RecordFile::onFileRead(
    size_t * bytesUsed,
    string_view data,
    bool more,
    int64_t offset,
    FileHandle f
) {
    bool good = ICarbonFileNotify::onFileRead(bytesUsed, data, more, offset, f);
    if (good && more)
        return true;
    appSignalShutdown();
    return false;
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    writeResults();
    tcLogShutdown(&s_opts.progress);
}


/****************************************************************************
*
*   Command line
*
***/

static bool analyzeCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("analyze")
        .desc("Analyze metrics from a recording.")
        .action(analyzeCmd);
    cli.opt(&reportfile, "<report file>")
        .desc("File to analyze, extension defaults to '.txt'");
    cli.opt(&ofile, "[output file]")
        .desc("'-' for stdout, otherwise extension defaults to '.txt'")
        .check([](auto & cli, auto & opt, auto & val) {
            if (*opt) {
                return opt->view() == "-"
                    ? true
                    : (bool) opt->defaultExt("txt");
            } else {
                // empty path not allowed
                return cli.badUsage("Missing argument", opt.from());
            }
        });

    cli.group("~").title("Other");

    cli.group("Output Options").sortKey("2");
    cli.opt(&openMode, "", FileAppendStream::kFail)
        .flagValue(true);
    cli.opt(&openMode, "truncate.", FileAppendStream::kTrunc)
        .desc("Truncate output file, if it exists.")
        .flagValue();
    cli.opt(&openMode, "append.", FileAppendStream::kAppend)
        .desc("Append to output file, if it exists.")
        .flagValue();
}

//===========================================================================
static bool analyzeCmd(Cli & cli) {
    if (s_opts.ofile.view() != "-") {
        s_file.init(10, 2, envMemoryConfig().pageSize);
        if (!s_file.open(s_opts.ofile.view(), s_opts.openMode))
            return cli.fail(EX_DATAERR, string(s_opts.ofile) + ": open failed");
    }

    consoleCatchCtrlC();
    shutdownMonitor(&s_cleanup);
    logMsgInfo() << "Analyzing " << s_opts.reportfile
        << " to " << s_opts.ofile;
    tcLogStart(&s_opts.progress);

    taskSetQueueThreads(taskComputeQueue(), 1);
    carbonInitialize();
    fileStreamBinary(&s_source, s_opts.reportfile, 4096);
    return cli.fail(EX_PENDING, "");
}
