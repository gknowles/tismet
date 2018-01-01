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
*   JsonCounters
*
***/

namespace {
class MetricIndex : public IHttpRouteNotify {
    void onHttpRequest(
        unsigned reqId,
        unordered_multimap<string_view, string_view> & params,
        HttpRequest & msg
    ) override;
};
} // namespace

//===========================================================================
void MetricIndex::onHttpRequest(
    unsigned reqId,
    unordered_multimap<string_view, string_view> & params,
    HttpRequest & msg
) {
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
*   Public API
*
***/

static MetricIndex s_index;

//===========================================================================
void tsGraphiteInitialize() {
    httpRouteAdd(&s_index, "/metrics/index.json", fHttpMethodGet);
}
