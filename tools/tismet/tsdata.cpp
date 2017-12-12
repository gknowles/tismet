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
    s_db = dbOpen(path);
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
