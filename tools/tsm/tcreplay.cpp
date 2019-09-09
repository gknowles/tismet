// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tcreply.cpp - tsm
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

namespace {

struct CmdOpts {
    string oaddr;
    DbProgressInfo progress;
    unsigned totalSecs;

    TimePoint startTime;
    TimePoint endTime;

    CmdOpts();
};

struct Metric {
    string name;
    double value;
    TimePoint time;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static SockMgrHandle s_mgr;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
inline static bool checkLimits(size_t moreBytes) {
    // Update counters and check thresholds, if exceeded roll back last value.
    s_opts.progress.bytes += moreBytes;
    s_opts.progress.samples += 1;
    if (s_opts.progress.totalBytes
            && s_opts.progress.bytes > s_opts.progress.totalBytes
        || s_opts.progress.totalSamples
            && s_opts.progress.samples > s_opts.progress.totalSamples
    ) {
        s_opts.progress.bytes -= moreBytes;
        s_opts.progress.samples -= 1;
        return false;
    }
    return true;
}


/****************************************************************************
*
*   AddrConn
*
***/

namespace {

class AddrConn : public IAppSocketNotify {
public:
    static constexpr size_t kBufferSize = 4096;

public:
    // Inherited via IAppSocketNotify
    void onSocketConnect(AppSocketInfo const & info) override;
    void onSocketConnectFailed() override;
    void onSocketDisconnect() override;
    bool onSocketRead(AppSocketData & data) override;
    void onSocketBufferChanged(AppSocketBufferInfo const & info) override;

private:
    void write();

    bool m_done{false};
    bool m_full{false};
};

} // namespace

//===========================================================================
void AddrConn::write() {
    string buffer;
    for (;;) {
        buffer.resize(kBufferSize);
        auto len = 0; // m_bufs.next(buffer.data(), buffer.size(), m_mets);
        if (!len)
            break;
        buffer.resize(len);
        socketWrite(this, buffer);
        if (m_full || m_done)
            return;
    }
    m_done = true;
}

//===========================================================================
void AddrConn::onSocketConnect(AppSocketInfo const & info) {
    write();
}

//===========================================================================
void AddrConn::onSocketConnectFailed() {
    logMsgInfo() << "Connect failed";
    appSignalShutdown();
}

//===========================================================================
void AddrConn::onSocketDisconnect() {
    if (!m_done) {
        logMsgInfo() << "Disconnect";
        m_done = true;
    }
    sockMgrSetAddresses(s_mgr, nullptr, 0);
    appSignalShutdown();
}

//===========================================================================
bool AddrConn::onSocketRead(AppSocketData & data) {
    return true;
}

//===========================================================================
void AddrConn::onSocketBufferChanged(AppSocketBufferInfo const & info) {
    if (info.waiting) {
        m_full = true;
    } else if (m_full && !info.waiting) {
        m_full = false;
        write();
    } else if (m_done
        && !info.incomplete
        && info.total == s_opts.progress.bytes
    ) {
        tcLogShutdown(&s_opts.progress);
        appSignalShutdown();
    }
}


/****************************************************************************
*
*   AddrJob
*
***/

namespace {

class AddrJob : ISockAddrNotify {
public:
    bool start(Cli & cli);

private:
    // Inherited via ISockAddrNotify
    void onSockAddrFound(SockAddr const * ptr, int count) override;

    int m_cancelId;
};

} // namespace

//===========================================================================
bool AddrJob::start(Cli & cli) {
    s_mgr = sockMgrConnect<AddrConn>("Metric Out");
    addressQuery(&m_cancelId, this, s_opts.oaddr, 2003);
    cli.fail(EX_PENDING, "");
    return true;
}

//===========================================================================
void AddrJob::onSockAddrFound(SockAddr const * ptr, int count) {
    if (!count) {
        appSignalShutdown();
    } else {
        logMsgInfo() << "Writing to " << s_opts.oaddr << " (" << *ptr << ")";
        tcLogStart(&s_opts.progress, (seconds) s_opts.totalSecs);
        sockMgrSetAddresses(s_mgr, ptr, count);
    }
    delete this;
}


/****************************************************************************
*
*   Command line
*
***/

static bool replayCmd(Cli & cli);

// 2001-01-01 12:00:00 UTC
constexpr TimePoint kDefaultStartTime{12'622'824'000s};

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("replay")
        .desc("Replay recorded metrics to carbon socket.")
        .action(replayCmd)
        .group("Target").sortKey("1")
        .title("Output Target");
    cli.opt(&oaddr, "A addr")
        .desc("Socket address to receive metrics, port defaults to 2003")
        .valueDesc("ADDRESS");

    cli.group("~").title("Other");

    cli.group("When to Stop").sortKey("2");
    cli.opt(&progress.totalBytes, "B bytes", 0)
        .desc("Max bytes to replay, 0 for all");
    cli.opt(&progress.totalSamples, "S samples", 0)
        .desc("Max samples to replay, 0 for all");
    cli.opt(&totalSecs, "T time", 0)
        .desc("Max seconds to replay, 0 for all");

    cli.group("Metrics to Replay").sortKey("3");
    cli.opt(&startTime, "s start", kDefaultStartTime)
        .desc("Start time of first sample")
        .valueDesc("TIME");
    cli.opt(&endTime, "e end")
        .desc("Time of last sample, rounded up to next interval")
        .valueDesc("TIME");
}

//===========================================================================
static bool replayCmd(Cli & cli) {
    AddrJob job;
    return cli.fail(EX_UNAVAILABLE, "replay not implemented");
    //auto job = make_unique<AddrJob>();
    //if (job->start(cli))
    //    job.release();
    //return false;
}
