// Copyright Glen Knowles 2018.
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
    auto mname = string(name); mname += "Path";
    out->member(mname, path);
    mname = name; mname += "SpaceAvail";
    auto ds = envDiskSpace(path);
    out->member(mname, ds.avail);
    mname = name; mname += "SpaceTotal";
    out->member(mname, ds.total);
}

//===========================================================================
void JsonAbout::onHttpRequest(unsigned reqId, HttpRequest & msg) {
    auto now = timeNow();
    HttpResponse res;
    JBuilder bld(&res.body());
    bld.object();
    bld.member("now", Time8601Str{now}.c_str());
    bld.member("version", tsProductVersion());
    bld.member("service", bool(appFlags() & fAppIsService));
    bld.member("startTime", Time8601Str{envProcessStartTime()}.c_str());
    bld.member("rootDir", appRootDir());
    addPath(&bld, "data", tsDataPath().parentPath());
    addPath(&bld, "log", appLogDir());
    addPath(&bld, "crash", appCrashDir());
    bld.member("config");
    configWriteRules(&bld);
    bld.member("account");
    envProcessAccount(&bld);
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

//===========================================================================
void tsWebInitialize() {
    httpRouteAdd(&s_jsonAbout, "/srv/about.json", fHttpMethodGet);

    resLoadWebSite("/admin");
    httpRouteAddRedirect("/", "/admin/");
}
