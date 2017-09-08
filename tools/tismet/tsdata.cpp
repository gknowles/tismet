// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Variables
*
***/

static TsdFileHandle s_tsd;


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
    tsdClose(s_tsd);
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
    s_tsd = tsdOpen(path);
}

//===========================================================================
TsdFileHandle tsDataHandle() {
    assert(s_tsd);
    return s_tsd;
}
