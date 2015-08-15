// error.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void LogMsg (DimErrorSeverity severity, const string & msg) {
    cout << msg << endl;
    if (severity == kCrash) {
        abort();
    }
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
