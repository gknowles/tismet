// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsgraphite.cpp - tismet
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

enum Format {
    kFormatInvalid,
    kFormatJson,
    kFormatMsgPack,
    kFormatPickle,
};

} // namespace

static TokenTable::Token s_formats[] = {
    { kFormatJson, "json" },
    { kFormatMsgPack, "msgpack" },
    { kFormatPickle, "pickle" },
};
static TokenTable s_formatTbl{s_formats, size(s_formats)};


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

//===========================================================================
static bool parseTime(TimePoint * abs, Duration * rel, string_view src) {
    char * eptr;
    auto t = strToInt64(src, &eptr);
    if (eptr < src.data() + src.size()) {
        if (!parse(rel, src))
            return false;
        *abs = {};
    } else {
        *abs = Clock::from_time_t(t);
        *rel = {};
    }
    return true;
}


/****************************************************************************
*
*   MetricIndex
*
***/

namespace {

class MetricIndex : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & req) override;
};

} // namespace

//===========================================================================
void MetricIndex::onHttpRequest(unsigned reqId, HttpRequest & req) {
    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h);
    vector<string_view> names;
    names.reserve(ids.size());
    for (auto && id : ids) {
        if (auto name = dbGetMetricName(h, id))
            names.push_back(name);
    }
    sort(names.begin(), names.end());

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/json");
    res.addHeader(kHttp_Status, "200");
    JBuilder bld(res.body());
    bld.array();
    for (auto && name : names) {
        started = xferIfFull(res, started, reqId, name.size() + 8);
        bld.value(name);
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

    void jsonReply(unsigned reqId, string_view target);
    void msgpackReply(unsigned reqId, string_view target);
};

} // namespace


//===========================================================================
void MetricFind::onHttpRequest(unsigned reqId, HttpRequest & req) {
    string format = "json";
    string target;
    for (auto && param : req.query().parameters) {
        if (param.values.empty())
            continue;
        if (param.name == "format") {
            format = param.values.front()->value;
        } else if (param.name == "query") {
            target = param.values.front()->value;
        }
    }
    if (target.empty())
        return httpRouteReply(reqId, req, 400, "Missing parameter: 'query'");
    auto ftype = tokenTableGetEnum(s_formatTbl, format.data(), kFormatInvalid);
    switch (ftype) {
    case kFormatJson:
        return jsonReply(reqId, target);
    case kFormatMsgPack:
    case kFormatPickle:
        return msgpackReply(reqId, target);
    default:
        return httpRouteReply(
            reqId,
            req,
            400,
            "Missing or unknown format: '"s + format + "'"
        );
    }
}

//===========================================================================
void MetricFind::jsonReply(unsigned reqId, string_view target) {
    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h, target);
    UnsignedSet bids;
    dbFindBranches(bids, h, target);

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/json");
    res.addHeader(kHttp_Status, "200");
    JBuilder bld(res.body());
    bld.array();
    for (auto && bid : bids) {
        if (auto name = dbGetBranchName(h, bid)) {
            auto namev = string_view{name};
            if (auto pos = namev.find_last_of('.'); pos != namev.npos)
                namev.remove_prefix(pos + 1);
            started = xferIfFull(res, started, reqId, namev.size() + 16);
            bld.object();
            bld.member("text", namev);
            bld.member("expandable", true);
            bld.end();
        }
    }
    for (auto && id : ids) {
        MetricInfo info;
        auto name = dbGetMetricName(h, id);
        if (name && dbGetMetricInfo(info, h, id)) {
            auto namev = string_view{name};
            if (auto pos = namev.find_last_of('.'); pos != namev.npos)
                namev.remove_prefix(pos + 1);
            started = xferIfFull(res, started, reqId, namev.size() + 32);
            bld.object();
            bld.member("text", namev);
            bld.member("expandable", false);
            bld.end();
        }
    }
    bld.end();
    xferRest(res, started, reqId);
}

//===========================================================================
void MetricFind::msgpackReply(unsigned reqId, string_view target) {
    auto h = tsDataHandle();
    UnsignedSet ids;
    dbFindMetrics(ids, h, target);
    UnsignedSet bids;
    dbFindBranches(bids, h, target);

    bool started = false;
    HttpResponse res;
    res.addHeader(kHttpContentType, "application/x-msgpack");
    res.addHeader(kHttp_Status, "200");
    MsgBuilder bld(res.body());
    auto count = ids.size() + bids.size();
    bld.array(count);
    for (auto && bid : bids) {
        if (auto name = dbGetBranchName(h, bid)) {
            auto namev = string_view{name};
            started = xferIfFull(res, started, reqId, namev.size() + 16);
            bld.map(2);
            bld.element("path", namev);
            bld.element("is_leaf", false);
        }
    }
    for (auto && id : ids) {
        MetricInfo info;
        auto name = dbGetMetricName(h, id);
        if (name && dbGetMetricInfo(info, h, id)) {
            auto namev = string_view{name};
            started = xferIfFull(res, started, reqId, namev.size() + 32);
            auto withInterval = (bool) info.first;
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
    assert(bld.depth() == 0);
    xferRest(res, started, reqId);
}


/****************************************************************************
*
*   Render
*
***/

namespace {

class Render : public IHttpRouteNotify {
    void onHttpRequest(unsigned reqId, HttpRequest & msg) override;
};

class RenderMsgPack : IDbEnumNotify {
public:
    RenderMsgPack(
        unsigned reqId,
        TimePoint from,
        TimePoint until,
        vector<string> & targets,
        vector<UnsignedSet> & idSets
    );

private:
    bool OnDbMetricStart(
        uint32_t id,
        string_view name,
        DbSampleType type,
        TimePoint from,
        TimePoint until,
        Duration interval
    ) override;
    bool OnDbSample(TimePoint time, double value) override;

    unsigned m_reqId{0};
    bool m_started{false};
    HttpResponse m_res;

    MsgBuilder m_bld{m_res.body()};
    string_view m_pathExpr;
    TimePoint m_prevTime;
    Duration m_interval;
};

class RenderJson : IDbEnumNotify {
public:
    RenderJson(
        unsigned reqId,
        TimePoint from,
        TimePoint until,
        vector<string> & targets,
        vector<UnsignedSet> & idSets
    );

private:
    bool OnDbMetricStart(
        uint32_t id,
        string_view name,
        DbSampleType type,
        TimePoint from,
        TimePoint until,
        Duration interval
    ) override;
    void OnDbMetricEnd(uint32_t id) override;
    bool OnDbSample(TimePoint time, double value) override;

    unsigned m_reqId{0};
    bool m_started{false};
    HttpResponse m_res;

    JBuilder m_bld{m_res.body()};
    string_view m_pathExpr;
    TimePoint m_prevTime;
    Duration m_interval;
};

} // namespace

//===========================================================================
// Render
//===========================================================================
void Render::onHttpRequest(unsigned reqId, HttpRequest & req) {
    string format;
    vector<string> targets;
    TimePoint from;
    TimePoint until;
    TimePoint now;
    Duration relFrom;
    Duration relUntil;

    for (auto && param : req.query().parameters) {
        if (param.values.empty())
            continue;
        if (param.name == "format") {
            format = param.values.front()->value;
        } else if (param.name == "target") {
            for (auto && val : param.values)
                targets.emplace_back(val.value);
        } else if (param.name == "now") {
            auto t = strToInt64(param.values.front()->value);
            now = Clock::from_time_t(t);
        } else if (param.name == "from") {
            if (!parseTime(&from, &relFrom, param.values.front()->value)) {
                return httpRouteReply(
                    reqId,
                    req,
                    400,
                    "Invalid parameter: 'from'"
                );
            }
        } else if (param.name == "until") {
            if (!parseTime(&until, &relUntil, param.values.front()->value)) {
                return httpRouteReply(
                    reqId,
                    req,
                    400,
                    "Invalid parameter: 'until'"
                );
            }
        }
    }
    if (targets.empty())
        return httpRouteReply(reqId, req, 400, "Missing parameter: 'target'");

    auto ftype = tokenTableGetEnum(s_formatTbl, format.data(), kFormatInvalid);
    switch (ftype) {
    case kFormatJson:
        break;
    case kFormatMsgPack:
    case kFormatPickle:
        ftype = kFormatMsgPack;
        break;
    default:
        return httpRouteReply(
            reqId,
            req,
            400,
            "Missing or unknown format: '"s + format + "'"
        );
    }

    if (!now)
        now = Clock::now();
    if (!from)
        from = now + relFrom;
    if (!until)
        until = now + relUntil;

    auto h = tsDataHandle();
    vector<UnsignedSet> idSets;
    for (auto && target : targets) {
        UnsignedSet & out = idSets.emplace_back();
        dbFindMetrics(out, h, target);
    }

    if (ftype == kFormatMsgPack) {
        RenderMsgPack render(reqId, from, until, targets, idSets);
    } else {
        assert(ftype == kFormatJson);
        RenderJson render(reqId, from, until, targets, idSets);
    }
}

//===========================================================================
// RenderMsgPack
//===========================================================================
RenderMsgPack::RenderMsgPack(
    unsigned reqId,
    TimePoint from,
    TimePoint until,
    vector<string> & targets,
    vector<UnsignedSet> & idSets
)
    : m_reqId{reqId}
{
    m_res.addHeader(kHttpContentType, "application/x-msgpack");
    m_res.addHeader(kHttp_Status, "200");

    auto h = tsDataHandle();
    UnsignedSet ids;
    size_t count{0};
    if (targets.size() == 1) {
        count = idSets.front().size();
    } else {
        for (auto && iset : idSets)
            ids.insert(iset);
        count = ids.size();
        ids.clear();
    }
    m_bld.array(count);
    for (unsigned i = 0; i < targets.size(); ++i) {
        m_pathExpr = targets[i];
        auto & iset = idSets[i];
        iset.erase(ids);
        for (auto && id : idSets[i]) {
            count = dbEnumSamples(this, h, id, from, until);
        }
        ids.insert(move(iset));
    }
    assert(m_bld.depth() == 0);
    xferRest(m_res, m_started, reqId);
}

//===========================================================================
bool RenderMsgPack::OnDbMetricStart(
    uint32_t id,
    string_view name,
    DbSampleType type,
    TimePoint from,
    TimePoint until,
    Duration interval
) {
    m_bld.map(6);
    m_bld.element("name", name);
    m_bld.element("pathExpression", m_pathExpr);
    m_bld.element("start", Clock::to_time_t(from));
    m_bld.element("end", Clock::to_time_t(until));
    m_bld.element("step", duration_cast<seconds>(interval).count());
    m_bld.element("values");
    auto count = (until - from) / interval;
    m_bld.array(count);
    m_prevTime = from - interval;
    m_interval = interval;
    return true;
}

//===========================================================================
bool RenderMsgPack::OnDbSample(TimePoint time, double value) {
    auto count = size_t{1};
    if (time != m_prevTime + m_interval)
        count = (time - m_prevTime) / m_interval;
    m_started = xferIfFull(m_res, m_started, m_reqId, 4 + count);
    for (; count > 1; --count)
        m_bld.value(nullptr);
    m_bld.value(value);
    m_prevTime = time;
    return true;
}

//===========================================================================
// RenderJson
//===========================================================================
RenderJson::RenderJson(
    unsigned reqId,
    TimePoint from,
    TimePoint until,
    vector<string> & targets,
    vector<UnsignedSet> & idSets
)
    : m_reqId{reqId}
{
    m_res.addHeader(kHttpContentType, "application/json");
    m_res.addHeader(kHttp_Status, "200");

    auto h = tsDataHandle();
    UnsignedSet ids;
    m_bld.array();
    for (unsigned i = 0; i < targets.size(); ++i) {
        m_pathExpr = targets[i];
        auto & iset = idSets[i];
        iset.erase(ids);
        for (auto && id : idSets[i]) {
            dbEnumSamples(this, h, id, from, until);
        }
        ids.insert(move(iset));
    }
    m_bld.end();
    xferRest(m_res, m_started, reqId);
}

//===========================================================================
bool RenderJson::OnDbMetricStart(
    uint32_t id,
    string_view name,
    DbSampleType type,
    TimePoint from,
    TimePoint until,
    Duration interval
) {
    if (from == until)
        return false;

    m_bld.object();
    m_bld.member("target", name);
    m_bld.member("datapoints");
    m_bld.array();
    return true;
}

//===========================================================================
bool RenderJson::OnDbSample(TimePoint time, double value) {
    m_bld.array();
    m_bld.value(value);
    m_bld.ivalue(Clock::to_time_t(time));
    m_bld.end();
    return true;
}

//===========================================================================
void RenderJson::OnDbMetricEnd(uint32_t id) {
    m_bld.end();
    m_bld.end();
}


/****************************************************************************
*
*   Public API
*
***/

static MetricIndex s_index;
static MetricFind s_find;
static Render s_render;

//===========================================================================
void tsGraphiteInitialize() {
    httpRouteAdd(&s_index, "/metrics/index.json", fHttpMethodGet);
    httpRouteAdd(&s_find, "/metrics/find", fHttpMethodGet);
    httpRouteAdd(&s_find, "/metrics/find/", fHttpMethodGet);
    httpRouteAdd(&s_render, "/render", fHttpMethodGet);
    httpRouteAdd(&s_render, "/render/", fHttpMethodGet);
    httpRouteAdd(&s_render, "/render", fHttpMethodPost);
}
