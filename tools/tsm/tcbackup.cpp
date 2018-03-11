// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tcbackup.cpp - tsm
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
    bool wait;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;

static DbProgressInfo s_info;

static SockMgrHandle s_mgr;
static int s_statusLines;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void logStart(string_view target, const Endpoint * addr) {
    tcLogStart();
    auto os = logMsgInfo();
    os << "Backing up server at " << target;
    if (addr)
        os << " (" << *addr << ")";
}

//===========================================================================
static void logShutdown() {
    if (++s_statusLines > 1)
        consoleRedoLine();
    if (!s_opts.wait) {
        logMsgInfo() << "Started";
    } else {
        tcLogShutdown(&s_info);
    }
}

//===========================================================================
static bool reportStatus(XNode * node) {
    auto val = attrValue(node, "status");
    if (!val)
        return false;
    auto mode = fromString(val, kRunStopping);
    DbProgressInfo info = {};
    auto e = firstChild(node, "Files");
    info.files = strToInt(attrValue(e, "value", ""));
    info.totalFiles = strToInt(attrValue(e, "total", "-1"));
    e = firstChild(node, "Bytes");
    info.bytes = strToInt64(attrValue(e, "value", ""));
    info.totalBytes = strToInt64(attrValue(e, "total", "-1"));

    if (++s_statusLines > 1)
        consoleRedoLine();
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Working";
    if (mode == kRunStarting) {
        os << "; waiting for checkpoint to complete";
        return true;
    }

    os << "; files: " << s_info.files;
    if (s_info.totalFiles >= 0)
        os << " of " << s_info.totalFiles;
    if (s_info.bytes) {
        os << "; bytes: ";
        if (s_info.totalBytes >= 0) {
            auto pct = (double) s_info.bytes / (double) s_info.totalBytes * 100;
            os.precision(3);
            os << pct << "% of " << s_info.totalBytes;
        } else {
            os << s_info.bytes;
        }
    }
    return mode != kRunStopped;
}


/****************************************************************************
*
*   AddrConn
*
***/

namespace {

class AddrConn : public IAppSocketNotify, ITimerNotify {
public:
    static constexpr size_t kBufferSize = 4096;

public:
    // Inherited via IAppSocketNotify
    void onSocketConnect(const AppSocketInfo & info) override;
    void onSocketConnectFailed() override;
    void onSocketDisconnect() override;
    bool onSocketRead(AppSocketData & data) override;

    // Inherited via ITimerNotify
    Duration onTimer(TimePoint now) override;

private:
    void start();

    HttpConnHandle m_conn;
    RunMode m_mode{kRunStopped};
    DbProgressInfo m_info;
    int m_streamId{0};
    bool m_done{false};
};

} // namespace

//===========================================================================
void AddrConn::onSocketConnect(const AppSocketInfo & info) {
    CharBuf out;
    m_conn = httpConnect(&out);
    HttpRequest req;
    req.addHeaderRef(kHttp_Scheme, "http");
    req.addHeaderRef(kHttp_Authority, s_opts.oaddr.c_str());
    req.addHeaderRef(kHttp_Method, "POST");
    req.addHeaderRef(kHttp_Path, "/backup");
    m_streamId = httpRequest(&out, m_conn, req);
    socketWrite(this, out);
}

//===========================================================================
void AddrConn::onSocketConnectFailed() {
    logMsgError() << "Connect failed";
    appSignalShutdown();
}

//===========================================================================
void AddrConn::onSocketDisconnect() {
    httpClose(m_conn);
    m_conn = {};
    if (!m_done) {
        logMsgError() << "Disconnect";
        m_done = true;
    }
    sockMgrSetEndpoints(s_mgr, nullptr, 0);
    logShutdown();
    appSignalShutdown();
}

//===========================================================================
bool AddrConn::onSocketRead(AppSocketData & data) {
    CharBuf out;
    vector<unique_ptr<HttpMsg>> msgs;
    bool result = httpRecv(&out, &msgs, m_conn, data.data, data.bytes);
    if (!result)
        msgs.clear();
    for (auto && msg : msgs) {
        assert(!msg->isRequest());
        auto res = static_cast<HttpResponse *>(msg.get());
        XDocument doc;
        auto root = doc.parse(res->body().c_str());
        result = reportStatus(root);
        if (result) {
            if (s_opts.wait) {
                timerUpdate(this, 1s);
            } else {
                result = false;
            }
        }
        if (!result)
            m_done = true;
    }
    if (!out.empty())
        socketWrite(this, out);
    if (!result)
        socketDisconnect(this);
    return true;
}

//===========================================================================
Duration AddrConn::onTimer(TimePoint now) {
    CharBuf out;
    HttpRequest req;
    req.addHeaderRef(kHttp_Scheme, "http");
    req.addHeaderRef(kHttp_Authority, s_opts.oaddr.c_str());
    req.addHeaderRef(kHttp_Method, "GET");
    req.addHeaderRef(kHttp_Path, "/backup");
    m_streamId = httpRequest(&out, m_conn, req);
    socketWrite(this, out);
    return kTimerInfinite;
}



/****************************************************************************
*
*   AddrJob
*
***/

namespace {

class AddrJob : IEndpointNotify {
public:
    void start(Cli & cli);

private:
    // Inherited via IEndpointNotify
    void onEndpointFound(const Endpoint * ptr, int count) override;

    int m_cancelId;
};

} // namespace

static AddrJob s_job;

//===========================================================================
void AddrJob::start(Cli & cli) {
    s_mgr = sockMgrConnect<AddrConn>("Metric Out");
    endpointQuery(&m_cancelId, this, s_opts.oaddr, 2003);
    cli.fail(EX_PENDING, "");
}

//===========================================================================
void AddrJob::onEndpointFound(const Endpoint * ptr, int count) {
    if (!count) {
        appSignalShutdown();
    } else {
        logStart(s_opts.oaddr, ptr);
        sockMgrSetEndpoints(s_mgr, ptr, count);
    }
}


/****************************************************************************
*
*   Command line
*
***/

static bool backupCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("backup")
        .desc("Trigger backup of tismet server.")
        .action(backupCmd);
    cli.opt(&oaddr, "<address>")
        .desc("Address of server to backup, port defaults to 2003.")
        .valueDesc("ADDRESS");
    cli.opt(&wait, "wait", true)
        .desc("Wait for backup to finish before returning.");
}

//===========================================================================
static bool backupCmd(Cli & cli) {
    s_job.start(cli);
    return false;
}
