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
static auto & s_perfExpired = uperf("db.metrics expired");
static auto & s_perfIgnored = uperf("db.samples ignored (rule)");


/****************************************************************************
*
*   Metric defaults
*
***/

namespace {

struct MetricRule {
    regex pattern;
    Duration retention;
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
    bool onDbSeriesStart(DbSeriesInfo const & info) override;

    Duration timeUntilCheck();

    UnsignedSet m_ids;
    Duration m_expireInterval{};
};

} // namespace

static ExpireTimer s_expireTimer;

//===========================================================================
void ExpireTimer::updateInterval(Duration interval) {
    m_expireInterval = interval;
    timerUpdate(this, timeUntilCheck(), true);
}

//===========================================================================
Duration ExpireTimer::timeUntilCheck() {
    if (!m_expireInterval.count())
        return kTimerInfinite;
    auto now = timeNow();
    auto ticks = now.time_since_epoch().count();
    auto interval = m_expireInterval.count();
    auto wait = Duration{interval - ticks % interval};
    return wait;
}

//===========================================================================
Duration ExpireTimer::onTimer(TimePoint now) {
    if (appStopping())
        return kTimerInfinite;

    if (!m_ids)
        dbFindMetrics(&m_ids, s_db);

    if (m_ids) {
        auto id = m_ids.pop_front();
        dbGetMetricInfo(this, s_db, id);
        return kTimerInfinite;
    }

    return timeUntilCheck();
}

//===========================================================================
bool ExpireTimer::onDbSeriesStart(DbSeriesInfo const & info) {
    if (!info.last)
        return true;

    auto now = timeNow();
    if (now >= info.first + 2 * (info.last - info.first)) {
        s_perfExpired += 1;
        dbEraseMetric(s_db, info.id);
    }

    auto wait = m_ids ? 1ms : timeUntilCheck();
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
    void onConfigChange(XDocument const & doc) override;
};

} // namespace

static AppXmlNotify s_appXml;

//===========================================================================
void AppXmlNotify::onConfigChange(XDocument const & doc) {
    if (s_db) {
        DbConfig conf;
        conf.checkpointMaxData =
            (size_t) configNumber(doc, "CheckpointMaxData");
        conf.checkpointMaxInterval =
            configDuration(doc, "CheckpointMaxInterval");
        dbConfigure(s_db, conf);
    }

    Duration val =
        configDuration(doc, "MetricExpirationCheckInterval", 24h);
    // In addition to the range of 5 minutes to a week, a check interval
    // of 0 (disable checking) is also allowed.
    if (val.count())
        val = clamp(val, Duration{5min}, Duration{168h});
    if (!s_db)
        val = 0ms;
    s_expireTimer.updateInterval(val);

    s_rules.clear();
    auto xdefs = configElement(doc, "MetricDefaults");
    for (auto && xrule : elems(xdefs, "Rule")) {
        MetricRule rule{};
        auto val = attrValue(&xrule, "pattern", "");
        try {
            rule.pattern.assign(val, regex::nosubs|regex::optimize);
        } catch(exception &) {
            logMsgError() << "Invalid metric rule pattern, " << val;
            continue;
        }
        (void) parse(&rule.retention, attrValue(&xrule, "retention", ""));
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

static Path s_dbPath;

//===========================================================================
Path const & tsDataPath() {
    return s_dbPath;
}

//===========================================================================
void tsDataInitialize() {
    shutdownMonitor(&s_cleanup);
    configMonitor("app.xml", &s_appXml);
    appDataPath(&s_dbPath, "metrics");
    s_db = dbOpen(s_dbPath, 0, fDbOpenVerbose | fDbOpenCreat);
    if (!s_db) {
        logMsgError() << "Unable to open database, " << s_dbPath;
        return appSignalShutdown(EX_DATAERR);
    }
    configChange("app.xml", &s_appXml);
}

//===========================================================================
void tsDataBackup(IDbProgressNotify * notify) {
    Path path;
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
bool tsDataInsertMetric(uint32_t * id, DbHandle f, string_view name) {
    assert(f);
    if (dbFindMetric(id, f, name))
        return true;

    DbMetricInfo info = {};
    bool found = false;
    for (auto && rule : s_rules) {
        if (regex_search(name.begin(), name.end(), rule.pattern)) {
            found = true;
            info.retention = rule.retention;
            break;
        }
    }
    if (found && !info.retention.count()) {
        if (name.substr(0, 7) != "tismet.")
            s_perfIgnored += 1;
        return false;
    }
    dbInsertMetric(id, f, name);
    if (found)
        dbUpdateMetric(f, *id, info);
    return true;
}
