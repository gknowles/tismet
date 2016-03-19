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
*   LogInternal
*
***/

class Detail::LogInternal {
public:
    LogInternal (LogType type) : m_type(type) {}
    LogType m_type;
};


/****************************************************************************
*
*   Log
*
***/

//===========================================================================
Detail::Log::Log (const LogInternal & from) 
    : m_type(from.m_type)
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
    return Detail::LogInternal{kLogDebug};
}

//===========================================================================
Detail::Log logMsgInfo () {
    return Detail::LogInternal{kLogInfo}; 
}

//===========================================================================
Detail::Log logMsgError () {
    return Detail::LogInternal{kLogError}; 
}

//===========================================================================
Detail::LogCrash logMsgCrash () {
    return Detail::LogInternal{kLogCrash}; 
}


} // namespace
