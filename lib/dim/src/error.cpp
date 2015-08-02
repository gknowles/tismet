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
void LogMsg (DimErrorSeverity severity, const string & msg) {
    printf("%s\n", msg.c_str());
    if (severity == kFatal) {
        abort();
    }
}


/****************************************************************************
*
*   DimErrorLog
*
***/

//===========================================================================
DimErrorLog::~DimErrorLog () {
    LogMsg(m_severity, str());
}
