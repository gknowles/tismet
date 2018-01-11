// Copyright Glen Knowles 2017.
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
*   Expire old metrics
*
***/

namespace {

class ExpireTimer : public ITimerNotify, public ITaskNotify {
public:
    void updateInterval(Duration interval);

private:
    Duration onTimer(TimePoint now) override;
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
        dbFindMetrics(m_ids, s_db);

    if (!m_ids.empty()) {
        auto id = m_ids.pop_front();
        MetricInfo info;
        if (dbGetMetricInfo(info, s_db, id)) {
            if (now >= info.first + 2 * info.retention) {
                s_perfExpired += 1;
                dbEraseMetric(s_db, id);
            }
        }
    }

    if (!m_ids.empty())
        return 1ms;

    return timeUntilCheck();
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
    conf.checkpointMaxData = configUnsigned(doc, "CheckpointMaxData");
    conf.checkpointMaxInterval =
        (seconds) configUnsigned(doc, "CheckpointMaxInterval");
    conf.pageMaxAge = (seconds) configUnsigned(doc, "WorkMemoryMaxAge");
    conf.pageScanInterval =
        (seconds) configUnsigned(doc, "WorkMemoryScanInterval");
    if (s_db)
        dbConfigure(s_db, conf);

    Duration val = (seconds) configUnsigned(
        doc,
        "ExpirationCheckInterval",
        (unsigned) duration_cast<seconds>(24h).count()
    );
    // In addition to the range of 5 minutes to a week, a check interval of 0
    // (disable checking) is also allowed.
    if (val.count())
        val = clamp(val, Duration{5min}, Duration{168h});
    s_expireTimer.updateInterval(val);
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
    string path;
    appDataPath(path, "metrics.dat");
    s_db = dbOpen(path, 0, fDbOpenVerbose);
    if (!s_db) {
        logMsgError() << "Unable to open database, " << path;
        return appSignalShutdown(EX_DATAERR);
    }
    configMonitor("app.xml", &s_appXml);
}

//===========================================================================
DbHandle tsDataHandle() {
    assert(s_db);
    return s_db;
}
