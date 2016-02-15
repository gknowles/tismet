// log.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Variables
*
***/

static vector<ILogNotify *> s_notifiers;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void LogMsg (LogSeverity severity, const string & msg) {
    if (s_notifiers.empty()) {
        cout << msg << endl;
    } else {
        for (auto&& notify : s_notifiers) {
            notify->onLog(severity, msg);
        }
    }

    if (severity == kCrash)
        abort();
}


/****************************************************************************
*
*   Log
*
***/

//===========================================================================
Log::~Log () {
    LogMsg(m_severity, str());
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
void logAddNotify (ILogNotify * notify) {
    s_notifiers.push_back(notify);
}

} // namespace
