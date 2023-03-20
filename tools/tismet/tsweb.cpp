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
*   JsonGraphite
*
***/

namespace {
class JsonGraphite : public IWebAdminNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};
} // namespace

//===========================================================================
void JsonGraphite::onHttpRequest(unsigned reqId, HttpRequest & msg) {
    auto res = HttpResponse(kHttpStatusOk);
    auto bld = initResponse(&res, reqId, msg);

    // Write routes.
    bld.member("routes").array();
    for (auto&& route : httpRouteGetRoutes()) {
        if (route.renderPath == "graphite" && !route.desc.empty())
            httpRouteWrite(&bld, route);
    }
    bld.end();

    // Write functions.
    unordered_map<string_view, const TokenTable *> evalues;
    for (auto&& e : funcEnums())
        evalues[e.name] = e.table;

    bld.member("functions").array();
    for (auto&& f : funcFactories()) {
        bld.object();
        bld.member("name", f.m_names[0]);
        if (f.m_names.size() > 1) {
            bld.member("aliases").array();
            for (auto i = 1; i < f.m_names.size(); ++i) {
                bld.value(f.m_names[i]);
            }
            bld.end();
        }
        bld.member("group", f.m_group);
        if (!f.m_args.empty()) {
            bld.member("args").array();
            for (auto & arg : f.m_args) {
                bld.object();
                bld.member("name", arg.name);
                bld.member("type", toString(arg.type));
                if (arg.require)
                    bld.member("require", true);
                if (arg.multiple)
                    bld.member("multiple", true);
                if (arg.type == Eval::FuncArg::kEnum) {
                    bld.member("values").array();
                    for (auto & v : *evalues[arg.enumName])
                        bld.value(v.name);
                    bld.end();
                }
                bld.end();
            }
            bld.end();
        }
        bld.end();
    }
    bld.end();

    // Close and send reply.
    bld.end();
    httpRouteReply(reqId, move(res));
}


/****************************************************************************
*
*   Public API
*
***/

static JsonAbout s_jsonAbout;
static JsonGraphite s_jsonGraphite;

//===========================================================================
void tsWebInitialize() {
    httpRouteAdd({.notify = &s_jsonAbout, .path = "/srv/about.json"});
    httpRouteAdd({.notify = &s_jsonGraphite, .path = "/srv/graphite.json"});
}
