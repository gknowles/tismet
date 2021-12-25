// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// tcrecord.cpp - tsm
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
    Path ofile;
    FileAppendStream::OpenExisting openMode;

    DbProgressInfo progress;
    unsigned totalSecs;

    string addrStr;
    SockAddr addr;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;

static uint64_t s_samplesWritten;
static uint64_t s_bytesWritten;
static TimePoint s_startTime;

static FileAppendStream s_file;


/****************************************************************************
*
*   RecordTimer
*
***/

namespace {

class RecordTimer : public ITimerNotify {
    Duration onTimer(TimePoint now) override;
};

} // namespace

static RecordTimer s_timer;

//===========================================================================
Duration RecordTimer::onTimer(TimePoint now) {
    appSignalShutdown();
    return kTimerInfinite;
}


/****************************************************************************
*
*   RecordConn
*
***/

static SockMgrHandle s_mgr;

namespace {

class RecordConn : public ICarbonSocketNotify {
public:
    bool onCarbonValue(
        unsigned reqId,
        string_view name,
        TimePoint time,
        double value,
        uint32_t idHint
    ) override;
private:
    string m_buf;
};

} // namespace

//===========================================================================
bool RecordConn::onCarbonValue(
    unsigned reqId,
    string_view name,
    TimePoint time,
    double value,
    uint32_t idHint
) {
    if (appStopping())
        return true;

    m_buf.clear();
    carbonWrite(m_buf, name, time, (float) value);
    s_bytesWritten += m_buf.size();
    if (s_opts.progress.totalBytes
        && s_opts.progress.bytes > s_opts.progress.totalBytes
    ) {
        s_bytesWritten -= m_buf.size();
        appSignalShutdown();
        return true;
    }
    s_samplesWritten += 1;

    if (s_file) {
        s_file.append(m_buf);
    } else {
        cout << m_buf;
    }

    if (s_opts.progress.totalSamples
            && s_opts.progress.samples == s_opts.progress.totalSamples
        || s_opts.progress.totalBytes
            && s_opts.progress.bytes == s_opts.progress.totalBytes
    ) {
        appSignalShutdown();
    }
    return true;
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
    s_file.close();
    tcLogShutdown(&s_opts.progress);
}


/****************************************************************************
*
*   Command line
*
***/

static bool recordCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("record")
        .desc("Create recording of metrics received via carbon protocol.")
        .action(recordCmd);
    cli.opt(&ofile, "<output file>")
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
    cli.opt(&addrStr, "[address]", "127.0.0.1:2003")
        .desc("Socket address to listen on")
        .after([](auto & cli, auto & opt, auto & val) {
            return parse(&s_opts.addr, *opt, 2003)
                || cli.badUsage(opt, *opt);
        });

    cli.group("~").title("Other");

    cli.group("When to Stop").sortKey("1");
    cli.opt(&progress.totalBytes, "B bytes", 0)
        .desc("Max bytes to record, 0 for unlimited");
    cli.opt(&progress.totalSamples, "S samples", 0)
        .desc("Max samples to record, 0 for unlimited");
    cli.opt(&totalSecs, "T time", 0)
        .desc("Max seconds to record, 0 for unlimited");

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
static bool recordCmd(Cli & cli) {
    if (s_opts.ofile.view() != "-") {
        s_file.init(10, 2, envMemoryConfig().pageSize);
        if (!s_file.open(s_opts.ofile.view(), s_opts.openMode))
            return cli.fail(EX_DATAERR, string(s_opts.ofile) + ": open failed");
    }

    consoleCatchCtrlC();
    shutdownMonitor(&s_cleanup);
    logMsgInfo() << "Recording " << s_opts.addr << " to " << s_opts.ofile;
    tcLogStart(&s_opts.progress, (chrono::seconds) s_opts.totalSecs);
    if (s_opts.totalSecs)
        timerUpdate(&s_timer, (chrono::seconds) s_opts.totalSecs);

    taskSetQueueThreads(taskComputeQueue(), 1);
    carbonInitialize();
    s_mgr = sockMgrListen<RecordConn>(
        "CarbonCli",
        (AppSocket::Family) TismetSocket::kCarbon
    );
    sockMgrSetAddresses(s_mgr, &s_opts.addr, 1);

    return cli.fail(EX_PENDING, "");
}
