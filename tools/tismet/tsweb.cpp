// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// tsweb.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   JsonAbout
*
***/

namespace {
class JsonAbout : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};
} // namespace

//===========================================================================
static void addPath(IJBuilder * out, string_view name, string_view path) {
    out->member(name);
    out->object();
    out->member("path", path);
    auto ds = envDiskSpace(path);
    out->member("spaceAvail", ds.avail);
    out->member("spaceTotal", ds.total);
    out->end();
}

//===========================================================================
void JsonAbout::onHttpRequest(unsigned reqId, HttpRequest & msg) {
    auto now = timeNow();
    HttpResponse res;
    JBuilder bld(&res.body());
    bld.object();
    bld.member("now", now);
    bld.member("version", tsProductVersion());
    bld.member("service", appFlags().any(fAppIsService));
    bld.member("startTime", envProcessStartTime());
    bld.member("rootDir", appRootDir());
    addPath(&bld, "dataDir", tsDataPath().parentPath());
    addPath(&bld, "logDir", appLogDir());
    addPath(&bld, "crashDir", appCrashDir());
    bld.member("config");
    configWriteRules(&bld);
    bld.member("account");
    envProcessAccountInfo(&bld);
    bld.end();
    res.addHeader(kHttpContentType, "application/json");
    res.addHeader(kHttp_Status, "200");
    httpRouteReply(reqId, move(res));
}


/****************************************************************************
*
*   Public API
*
***/

static JsonAbout s_jsonAbout;
static HttpRouteRedirectNotify s_redirectAdmin("/admin/");

//===========================================================================
void tsWebInitialize() {
    httpRouteAdd({.notify = &s_jsonAbout, .path = "/srv/about.json"});

    resLoadWebSite("/admin", {}, resWebSiteContent());
    httpRouteAdd({.notify = &s_redirectAdmin, .path = "/"});
    httpRouteAddAlias(
        { .path = "/admin", .recurse = true },
        "/admin"
    );
}
