// error.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Variables
*
***/

static vector<IDimLogNotify *> s_notifiers;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void LogMsg (DimLogSeverity severity, const string & msg) {
    if (s_notifiers.empty()) {
        cout << msg << endl;
    } else {
        for (auto&& notify : s_notifiers) {
            notify->OnLog(severity, msg);
        }
    }

    if (severity == kCrash)
        abort();
}


/****************************************************************************
*
*   DimLog
*
***/

//===========================================================================
DimLog::~DimLog () {
    LogMsg(m_severity, str());
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
void DimLogRegisterHandler (IDimLogNotify * notify) {
    s_notifiers.push_back(notify);
}
