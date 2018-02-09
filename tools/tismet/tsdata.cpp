// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   Variables
*
***/

static DbHandle s_db;
static auto & s_perfExpired = uperf("db metrics expired");


/****************************************************************************
*
*   Metric defaults
*
***/

namespace {

struct MetricRule {
    regex pattern;
    Duration retention;
    Duration interval;
    DbSampleType type;
};

} // namespace

static vector<MetricRule> s_rules;



/****************************************************************************
*
*   Expire old metrics
*
***/

namespace {

class ExpireTimer : public ITimerNotify, public IDbDataNotify {
public:
    void updateInterval(Duration interval);

private:
    Duration onTimer(TimePoint now) override;
    bool onDbSeriesStart(const DbSeriesInfo & info) override;

    Duration timeUntilCheck();

    UnsignedSet m_ids;
    Duration m_expireInterval;
};

} // namespace

static ExpireTimer s_expireTimer;

//===========================================================================
void ExpireTimer::updateInterval(Duration interval) {
    m_expireInterval = interval;
    timerUpdate(this, timeUntilCheck());
}

//===========================================================================
Duration ExpireTimer::timeUntilCheck() {
    if (!m_expireInterval.count())
        return kTimerInfinite;
    auto now = Clock::now();
    auto ticks = now.time_since_epoch().count();
    auto interval = m_expireInterval.count();
    auto wait = Duration{interval - ticks % interval};
    return wait;
}

//===========================================================================
Duration ExpireTimer::onTimer(TimePoint now) {
    if (m_ids.empty())
        dbFindMetrics(&m_ids, s_db);

    if (!m_ids.empty()) {
        auto id = m_ids.pop_front();
        dbGetMetricInfo(this, s_db, id);
        return kTimerInfinite;
    }

    return timeUntilCheck();
}

//===========================================================================
bool ExpireTimer::onDbSeriesStart(const DbSeriesInfo & info) {
    auto now = Clock::now();
    if (now >= info.first + 2 * (info.last - info.first)) {
        s_perfExpired += 1;
        dbEraseMetric(s_db, info.id);
    }

    auto wait = m_ids.empty() ? timeUntilCheck() : 1ms;
    timerUpdate(this, wait);
    return true;
}


/****************************************************************************
*
*   app.xml monitor
*
***/

namespace {

class AppXmlNotify : public IConfigNotify {
    void onConfigChange(const XDocument & doc) override;
};

} // namespace

static AppXmlNotify s_appXml;

//===========================================================================
void AppXmlNotify::onConfigChange(const XDocument & doc) {
    DbConfig conf;
    conf.checkpointMaxData = (size_t) configNumber(doc, "CheckpointMaxData");
    conf.checkpointMaxInterval = configDuration(doc, "CheckpointMaxInterval");
    conf.pageMaxAge = configDuration(doc, "WorkMemoryMaxAge");
    conf.pageScanInterval = configDuration(doc, "WorkMemoryScanInterval");
    if (s_db)
        dbConfigure(s_db, conf);

    Duration val = configDuration(doc, "MetricExpirationCheckInterval", 24h);
    // In addition to the range of 5 minutes to a week, a check interval of 0
    // (disable checking) is also allowed.
    if (val.count())
        val = clamp(val, Duration{5min}, Duration{168h});
    s_expireTimer.updateInterval(val);

    auto xdefs = configElement(doc, "MetricDefaults");
    for (auto && xrule : elems(xdefs, "Rule")) {
        MetricRule rule;
        auto val = attrValue(&xrule, "pattern", "");
        try {
            rule.pattern.assign(val, regex::nosubs|regex::optimize);
        } catch(exception &) {
            logMsgError() << "invalid metric rule pattern, " << val;
            continue;
        }
        val = attrValue(&xrule, "type", "");
        rule.type = fromString(val, kSampleTypeInvalid);
        (void) parse(&rule.retention, attrValue(&xrule, "retention", ""));
        (void) parse(&rule.interval, attrValue(&xrule, "interval", ""));
        s_rules.push_back(rule);
    }
}


/****************************************************************************
*
*   Shutdown monitor
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    dbClose(s_db);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsDataInitialize() {
    shutdownMonitor(&s_cleanup);
    configMonitor("app.xml", &s_appXml);
    string path;
    appDataPath(&path, "metrics");
    s_db = dbOpen(path, 0, fDbOpenVerbose);
    if (!s_db) {
        logMsgError() << "Unable to open database, " << path;
        return appSignalShutdown(EX_DATAERR);
    }
    configChange("app.xml", &s_appXml);
}

//===========================================================================
void tsDataBackup(IDbProgressNotify * notify) {
    string path;
    appDataPath(&path, "backup/metrics");
    dbBackup(notify, s_db, path);
}

//===========================================================================
DbHandle tsDataHandle() {
    assert(s_db);
    return s_db;
}

//===========================================================================
DbContextHandle tsDataOpenContext() {
    assert(s_db);
    return dbOpenContext(s_db);
}

//===========================================================================
bool tsDataInsertMetric(uint32_t * id, DbContextHandle ctx, string_view name) {
    assert(ctx);
    if (!dbInsertMetric(id, ctx, name))
        return false;

    for (auto && rule : s_rules) {
        if (regex_search(name.begin(), name.end(), rule.pattern)) {
            DbMetricInfo info = {};
            info.type = rule.type;
            info.retention = rule.retention;
            info.interval = rule.interval;
            dbUpdateMetric(ctx, *id, info);
            break;
        }
    }
    return true;
}
