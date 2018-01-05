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
    auto blksize = res.body().defaultBlockSize();
    JBuilder bld(res.body());
    bld.array();
    for (auto id : ids) {
        if (auto name = dbGetMetricName(h, id)) {
            auto namev = string_view{name};
            if (res.body().size() + namev.size() > blksize) {
                if (!started) {
                    httpRouteReply(reqId, res, true);
                    started = true;
                } else {
                    httpRouteReply(reqId, res.body(), true);
                }
                res.body().clear();
            }
            bld.value(name);
        }
    }
    bld.end();
    if (!started) {
        httpRouteReply(reqId, res);
    } else {
        httpRouteReply(reqId, res.body(), false);
    }
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
void MetricFind::onHttpRequest(unsigned reqId, HttpRequest & msg) {
    string format;
    string query;
    for (auto && param : msg.query().parameters) {
        if (param.name == "format") {
            if (!param.values.empty())
                format = param.values.front()->value;
        } else if (param.name == "query") {
            if (!param.values.empty())
                query = param.values.front()->value;
        }
    }

    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h, query);

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/x-msgpack");
    res.addHeader(kHttp_Status, "200");
    auto blksize = res.body().defaultBlockSize();
    MsgBuilder bld(res.body());
    auto count = ids.size();
    bld.array(count);
    for (auto id : ids) {
        MetricInfo info;
        auto name = dbGetMetricName(h, id);
        if (name && dbGetMetricInfo(info, h, id)) {
            auto namev = string_view{name};
            if (res.body().size() + namev.size() > blksize) {
                if (!started) {
                    httpRouteReply(reqId, res, true);
                    started = true;
                } else {
                    httpRouteReply(reqId, res.body(), true);
                }
                res.body().clear();
            }
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
    if (!started) {
        httpRouteReply(reqId, res);
    } else {
        httpRouteReply(reqId, res.body(), false);
    }
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
