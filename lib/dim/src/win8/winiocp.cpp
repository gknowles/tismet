// winiocp.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Incomplete public types
*
***/

/****************************************************************************
*
*   Private declarations
*
***/

namespace {

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static RunMode s_mode{kRunStopped};


/****************************************************************************
*
*   DimIocpShutdown
*
***/

namespace {
class DimIocpShutdown : public IDimAppShutdownNotify {
    bool OnAppQueryConsoleDestroy () override;
};
static DimIocpShutdown s_cleanup;
} // namespace

//===========================================================================
bool DimIocpShutdown::OnAppQueryConsoleDestroy () {
    s_mode = kRunStopping;
    s_mode = kRunStopped;
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimIocpInitialize () {
    s_mode = kRunStarting;
    DimAppMonitorShutdown(&s_cleanup);

    s_mode = kRunRunning;
}


/****************************************************************************
*
*   Public API
*
***/

