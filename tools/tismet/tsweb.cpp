// Copyright Glen Knowles 2018 - 2023.
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
class JsonAbout : public IWebAdminNotify {
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
    auto res = HttpResponse(kHttpStatusOk);
    auto bld = initResponse(&res, reqId, msg);
    addPath(&bld, "dataDir", tsDataPath().parentPath());
    addPath(&bld, "logDir", appLogDir());
    addPath(&bld, "crashDir", appCrashDir());
    configWriteRules(&bld, "config");
    bld.member("account").object();
    envProcessAccountInfo(&bld);
    bld.end();
    bld.end();
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
    httpRouteAdd({.notify = &s_jsonAbout, .path = "/srv/about.json"});
}
