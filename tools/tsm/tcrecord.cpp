// Copyright Glen Knowles 2017.
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
    FileAppendQueue::OpenExisting openMode;

    uint64_t maxBytes;
    unsigned maxSecs;
    uint64_t maxSamples;

    string addrStr;
    Endpoint addr;

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

static FileAppendQueue s_file;


//===========================================================================
static void logStart(string_view target, const Endpoint & source) {
    s_startTime = Clock::now();
    logMsgInfo() << "Recording " << source << " into " << target;
    if (s_opts.maxBytes || s_opts.maxSecs || s_opts.maxSamples) {
        auto os = logMsgInfo();
        os.imbue(locale(""));
        os << "Limits";
        if (auto num = s_opts.maxSamples)
            os << "; samples: " << num;
        if (auto num = s_opts.maxBytes)
            os << "; bytes: " << num;
        if (auto num = s_opts.maxSecs)
            os << "; seconds: " << num;
    }
}

//===========================================================================
static void logShutdown() {
    TimePoint finish = Clock::now();
    std::chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done; samples: " << s_samplesWritten
        << "; bytes: " << s_bytesWritten
        << "; seconds: " << elapsed.count();
}


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
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(uint32_t id, TimePoint time, double value) override;
private:
    string_view m_name;
    string m_buf;
};

} // namespace

//===========================================================================
uint32_t RecordConn::onCarbonMetric(string_view name) {
    m_name = name;
    return 1;
}

//===========================================================================
void RecordConn::onCarbonValue(uint32_t id, TimePoint time, double value) {
    assert(id == 1);
    if (appStopping())
        return;

    m_buf.clear();
    carbonWrite(m_buf, m_name, time, (float) value);
    s_bytesWritten += m_buf.size();
    if (s_opts.maxBytes && s_bytesWritten > s_opts.maxBytes) {
        s_bytesWritten -= m_buf.size();
        return appSignalShutdown();
    }
    s_samplesWritten += 1;

    if (s_file) {
        s_file.append(m_buf);
    } else {
        cout << m_buf;
    }

    if (s_opts.maxSamples && s_samplesWritten == s_opts.maxSamples
        || s_opts.maxBytes && s_bytesWritten == s_opts.maxBytes
    ) {
        return appSignalShutdown();
    }
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
    logShutdown();
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
    cli.opt(&maxBytes, "B bytes", 0)
        .desc("Max bytes to record, 0 for unlimited");
    cli.opt(&maxSecs, "T time", 0)
        .desc("Max seconds to record, 0 for unlimited");
    cli.opt(&maxSamples, "S samples", 0)
        .desc("Max samples to record, 0 for unlimited");

    cli.group("Output Options").sortKey("2");
    cli.opt(&openMode, "", FileAppendQueue::kFail)
        .flagValue(true);
    cli.opt(&openMode, "truncate.", FileAppendQueue::kTrunc)
        .desc("Truncate output file, if it exists.")
        .flagValue();
    cli.opt(&openMode, "append.", FileAppendQueue::kAppend)
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
    logStart(s_opts.ofile, s_opts.addr);
    if (s_opts.maxSecs)
        timerUpdate(&s_timer, (chrono::seconds) s_opts.maxSecs);

    taskSetQueueThreads(taskComputeQueue(), 1);
    carbonInitialize();
    s_mgr = sockMgrListen<RecordConn>(
        "CarbonCli",
        (AppSocket::Family) TismetSocket::kCarbon
    );
    sockMgrSetEndpoints(s_mgr, &s_opts.addr, 1);

    return cli.fail(EX_PENDING, "");
}
