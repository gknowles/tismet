// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsbackup.cpp - tismet
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

/****************************************************************************
*
*   Private
*
***/


/****************************************************************************
*
*   BackupProgress
*
***/

namespace {

struct BackupProgress : IDbProgressNotify {
    RunMode m_mode{kRunStopped};
    DbProgressInfo m_info;

    bool OnDbProgress(RunMode mode, const DbProgressInfo & info) override;
};

} // namespace

static BackupProgress s_progress;

//===========================================================================
bool BackupProgress::OnDbProgress(RunMode mode, const DbProgressInfo & info) {
    m_mode = mode;
    m_info = info;
    return true;
}


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
inline static bool xferIfFull(
    HttpResponse & res,
    bool started,
    unsigned reqId,
    size_t pending
) {
    auto blksize = res.body().defaultBlockSize();
    if (res.body().size() + pending > blksize) {
        if (!started) {
            httpRouteReply(reqId, res, true);
            started = true;
        } else {
            httpRouteReply(reqId, res.body(), true);
        }
        res.body().clear();
    }
    return started;
}

//===========================================================================
inline static void xferRest(
    HttpResponse & res,
    bool started,
    unsigned reqId
) {
    if (!started) {
        httpRouteReply(reqId, res);
    } else {
        httpRouteReply(reqId, res.body(), false);
    }
}

//===========================================================================
static void replyStatus(unsigned reqId) {
    auto & info = s_progress.m_info;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/xml");
    res.addHeader(kHttp_Status, "200");
    XBuilder bld(res.body());
    bld.start("Backup").attr("status", toString(s_progress.m_mode));
    StrFrom<size_t> str;
    bld.start("Metrics").attr("value", str.set(info.metrics).data());
    if (info.totalMetrics != size_t(-1))
        bld.attr("total", str.set(info.totalMetrics).data());
    bld.end();
    bld.start("Samples").attr("value", str.set(info.metrics).data());
    if (info.totalSamples != size_t(-1))
        bld.attr("total", str.set(info.totalSamples).data());
    bld.end();
    bld.start("Bytes").attr("value", str.set(info.bytes).data());
    if (info.totalBytes != size_t(-1))
        bld.attr("total", str.set(info.totalBytes).data());
    bld.end();
    bld.end();
    httpRouteReply(reqId, res);
}


/****************************************************************************
*
*   BackupStart
*
***/

namespace {

class BackupStart : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & req) override;
};

} // namespace

//===========================================================================
void BackupStart::onHttpRequest(unsigned reqId, HttpRequest & req) {
    tsBackupStart();
    replyStatus(reqId);
}


/****************************************************************************
*
*   BackupQuery
*
***/

namespace {

class BackupQuery : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};

} // namespace

//===========================================================================
void BackupQuery::onHttpRequest(unsigned reqId, HttpRequest & req) {
    replyStatus(reqId);
}


/****************************************************************************
*
*   Public API
*
***/

static BackupStart s_backStart;
static BackupQuery s_backQuery;

//===========================================================================
void tsBackupInitialize() {
    httpRouteAdd(&s_backStart, "/backup", fHttpMethodPost);
    httpRouteAdd(&s_backQuery, "/backup", fHttpMethodGet);
}

//===========================================================================
void tsBackupStart() {
    tsDataBackup(&s_progress);
}
