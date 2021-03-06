// Copyright Glen Knowles 2018.
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
*   BackupProgress
*
***/

namespace {

class BackupProgress : public IDbProgressNotify {
public:
    static void buildResponse(
        HttpResponse * out,
        BackupProgress const & progress
    );

public:
    void replyStatus(unsigned reqId, bool immediate);

private:
    bool onDbProgress(RunMode mode, DbProgressInfo const & info) override;
    void copy_LK(BackupProgress * from) const;

    RunMode m_mode{kRunStopped};
    DbProgressInfo m_info{};
    Dim::TimePoint m_time;
    UnsignedSet m_reqIds;
    mutable mutex m_mut;
};

} // namespace

static BackupProgress s_progress;

//===========================================================================
static void addInfoElem(
    XBuilder & bld,
    string_view name,
    size_t value,
    size_t total
) {
    StrFrom<size_t> str;
    bld.start(name).attr("value", str.set(value).data());
    if (total != size_t(-1))
        bld.attr("total", str.set(total).data());
    bld.end();
}

//===========================================================================
// static
void BackupProgress::buildResponse(
    HttpResponse * out,
    BackupProgress const & progress
) {
    auto & info = progress.m_info;
    out->addHeader(kHttpContentType, "application/xml");
    out->addHeader(kHttp_Status, "200");
    XBuilder bld(&out->body());
    Time8601Str ts(progress.m_time, 3);
    bld.start("Backup")
        .attr("status", toString(progress.m_mode))
        .attr("time", ts.c_str());
    addInfoElem(bld, "Files", info.files, info.totalFiles);
    addInfoElem(bld, "Metrics", info.metrics, info.totalMetrics);
    addInfoElem(bld, "Samples", info.samples, info.totalSamples);
    addInfoElem(bld, "Bytes", info.bytes, info.totalBytes);
    bld.end();
}

//===========================================================================
void BackupProgress::replyStatus(unsigned reqId, bool immediate) {
    BackupProgress progress;
    {
        scoped_lock lk{m_mut};
        if (!immediate && m_mode != kRunStopped) {
            m_reqIds.insert(reqId);
            return;
        }

        copy_LK(&progress);
    }

    HttpResponse res;
    buildResponse(&res, progress);
    httpRouteReply(reqId, move(res));
}

//===========================================================================
void BackupProgress::copy_LK(BackupProgress * out) const {
    out->m_mode = m_mode;
    out->m_info = m_info;
    out->m_time = m_time;
    out->m_reqIds = m_reqIds;
}

//===========================================================================
bool BackupProgress::onDbProgress(RunMode mode, DbProgressInfo const & info) {
    BackupProgress progress;
    {
        scoped_lock lk{m_mut};
        m_mode = mode;
        m_info = info;
        m_time = timeNow();
        if (!m_reqIds)
            return true;

        copy_LK(&progress);
        m_reqIds.clear();
    }

    for (auto && reqId : progress.m_reqIds) {
        HttpResponse res;
        buildResponse(&res, progress);
        httpRouteReply(reqId, move(res));
    }
    return true;
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
    s_progress.replyStatus(reqId, true);
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
    s_progress.replyStatus(reqId, false);
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
    httpRouteAdd(&s_backQuery, "/backup");
}

//===========================================================================
void tsBackupStart() {
    tsDataBackup(&s_progress);
}
