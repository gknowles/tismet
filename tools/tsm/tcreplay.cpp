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
    uint64_t maxBytes;
    unsigned maxSecs;
    uint64_t maxSamples;

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

static uint64_t s_bytesWritten;
static uint64_t s_samplesWritten;
static TimePoint s_startTime;

static SockMgrHandle s_mgr;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void logStart(string_view target, const Endpoint * addr) {
    s_startTime = Clock::now();
    {
        auto os = logMsgInfo();
        os << "Writing to " << target;
        if (addr)
            os << " (" << *addr << ")";
    }
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

//===========================================================================
inline static bool checkLimits(size_t moreBytes) {
    // Update counters and check thresholds, if exceeded roll back last value.
    s_bytesWritten += moreBytes;
    s_samplesWritten += 1;
    if (s_opts.maxBytes && s_bytesWritten > s_opts.maxBytes
        || s_opts.maxSamples && s_samplesWritten > s_opts.maxSamples
    ) {
        s_bytesWritten -= moreBytes;
        s_samplesWritten -= 1;
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
    void onSocketConnect(const AppSocketInfo & info) override;
    void onSocketConnectFailed() override;
    void onSocketDisconnect() override;
    void onSocketRead(AppSocketData & data) override;
    void onSocketBufferChanged(const AppSocketBufferInfo & info) override;
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
void AddrConn::onSocketConnect(const AppSocketInfo & info) {
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
    sockMgrSetEndpoints(s_mgr, nullptr, 0);
    appSignalShutdown();
}

//===========================================================================
void AddrConn::onSocketRead(AppSocketData & data)
{}

//===========================================================================
void AddrConn::onSocketBufferChanged(const AppSocketBufferInfo & info) {
    if (info.waiting) {
        m_full = true;
    } else if (m_full && !info.waiting) {
        m_full = false;
        write();
    } else if (m_done
        && !info.incomplete
        && info.total == s_bytesWritten
    ) {
        logShutdown();
        appSignalShutdown();
    }
}


/****************************************************************************
*
*   AddrJob
*
***/

namespace {

class AddrJob : IEndpointNotify {
public:
    bool start(Cli & cli);

private:
    // Inherited via IEndpointNotify
    void onEndpointFound(const Endpoint * ptr, int count) override;

    int m_cancelId;
};

} // namespace

//===========================================================================
bool AddrJob::start(Cli & cli) {
    s_mgr = sockMgrConnect<AddrConn>("Metric Out");
    endpointQuery(&m_cancelId, this, s_opts.oaddr, 2003);
    cli.fail(EX_PENDING, "");
    return true;
}

//===========================================================================
void AddrJob::onEndpointFound(const Endpoint * ptr, int count) {
    if (!count) {
        appSignalShutdown();
    } else {
        logStart(s_opts.oaddr, ptr);
        sockMgrSetEndpoints(s_mgr, ptr, count);
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
        .desc("Replay recorded metrics to carbon endpoint.")
        .action(replayCmd)
        .group("Target").sortKey("1")
        .title("Output Target");
    cli.opt(&oaddr, "A addr")
        .desc("Socket endpoint to receive metrics, port defaults to 2003")
        .valueDesc("ADDRESS");

    cli.group("~").title("Other");

    cli.group("When to Stop").sortKey("2");
    cli.opt(&maxBytes, "B bytes", 0)
        .desc("Max bytes to replay, 0 for all");
    cli.opt(&maxSecs, "T time", 0)
        .desc("Max seconds to replay, 0 for all");
    cli.opt(&maxSamples, "S samples", 0)
        .desc("Max samples to replay, 0 for all");

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
    return cli.fail(EX_UNAVAILABLE, "replay not implemented");
    //auto job = make_unique<AddrJob>();
    //if (job->start(cli))
    //    job.release();
    //return false;
}
