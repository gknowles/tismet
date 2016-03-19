// log.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private
*
***/

static vector<ILogNotify *> s_notifiers;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void LogMsg (LogType type, const string & msg) {
    if (s_notifiers.empty()) {
        cout << msg << endl;
    } else {
        for (auto&& notify : s_notifiers) {
            notify->onLog(type, msg);
        }
    }

    if (type == kLogCrash)
        abort();
}


/****************************************************************************
*
*   Log
*
***/

//===========================================================================
Detail::Log::Log (LogType type) 
    : m_type(type)
{}

//===========================================================================
Detail::Log::~Log () {
    LogMsg(m_type, str());
}


/****************************************************************************
*
*   LogCrash
*
***/

//===========================================================================
Detail::LogCrash::~LogCrash () 
{}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
void logAddNotify (ILogNotify * notify) {
    s_notifiers.push_back(notify);
}

//===========================================================================
Detail::Log logMsgDebug () { 
    return kLogDebug;
}

//===========================================================================
Detail::Log logMsgInfo () {
    return kLogInfo; 
}

//===========================================================================
Detail::Log logMsgError () {
    return kLogError; 
}

//===========================================================================
Detail::LogCrash logMsgCrash () {
    return kLogCrash; 
}


} // namespace
