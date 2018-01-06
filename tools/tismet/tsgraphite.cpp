// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsgraphite.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool xferIfFull(
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
static void xferRest(
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


/****************************************************************************
*
*   MetricIndex
*
***/

namespace {
class MetricIndex : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};
} // namespace

//===========================================================================
void MetricIndex::onHttpRequest(unsigned reqId, HttpRequest & msg) {
    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h);

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/json");
    res.addHeader(kHttp_Status, "200");
    JBuilder bld(res.body());
    bld.array();
    for (auto id : ids) {
        if (auto name = dbGetMetricName(h, id)) {
            auto namev = string_view{name};
            started = xferIfFull(res, started, reqId, namev.size() + 8);
            bld.value(name);
        }
    }
    bld.end();
    xferRest(res, started, reqId);
}


/****************************************************************************
*
*   MetricFind
*
***/

namespace {
class MetricFind : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};
} // namespace

//===========================================================================
void MetricFind::onHttpRequest(unsigned reqId, HttpRequest & req) {
    string format;
    string query;
    for (auto && param : req.query().parameters) {
        if (param.name == "format") {
            if (!param.values.empty())
                format = param.values.front()->value;
        } else if (param.name == "query") {
            if (!param.values.empty())
                query = param.values.front()->value;
        }
    }
    if (query.empty())
        return httpRouteReply(reqId, req, 401, "Missing parameter, 'query'.");
    if (format != "pickle") {
        return httpRouteReply(
            reqId,
            req,
            401,
            "Missing or unknown format, '"s + format + "'."
        );
    }

    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h, query);
    UnsignedSet bids;
    dbFindBranches(bids, h, query);

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/x-msgpack");
    res.addHeader(kHttp_Status, "200");
    MsgBuilder bld(res.body());
    auto count = ids.size() + bids.size();
    bld.array(count);
    for (auto bid : bids) {
        if (auto name = dbGetBranchName(h, bid)) {
            auto namev = string_view{name};
            started = xferIfFull(res, started, reqId, namev.size() + 16);
            bld.map(2);
            bld.element("path", namev);
            bld.element("is_leaf", false);
        }
    }
    for (auto id : ids) {
        MetricInfo info;
        auto name = dbGetMetricName(h, id);
        if (name && dbGetMetricInfo(info, h, id)) {
            auto namev = string_view{name};
            started = xferIfFull(res, started, reqId, namev.size() + 32);
            bool withInterval = info.first != TimePoint{};
            bld.map(2 + withInterval);
            bld.element("path", namev);
            bld.element("is_leaf", true);
            if (withInterval) {
                auto start = Clock::to_time_t(info.first);
                auto end = Clock::to_time_t(
                    info.first + info.retention - info.interval
                );
                bld.element("intervals");
                bld.array(1);
                bld.array(2);
                bld.uvalue(start);
                bld.uvalue(end);
            }
        }
    }
    xferRest(res, started, reqId);
}


/****************************************************************************
*
*   Public API
*
***/

static MetricIndex s_index;
static MetricFind s_find;

//===========================================================================
void tsGraphiteInitialize() {
    httpRouteAdd(&s_index, "/metrics/index.json", fHttpMethodGet);
    httpRouteAdd(&s_find, "/metrics/find", fHttpMethodGet);
    httpRouteAdd(&s_find, "/metrics/find/", fHttpMethodGet);
}
