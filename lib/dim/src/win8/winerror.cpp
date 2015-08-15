// winerror.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   WinError
*
***/

//===========================================================================
WinError::WinError () {
    m_value = GetLastError();
}

//===========================================================================
std::ostream & operator<< (std::ostream & os, const WinError & val) {
    char buf[256];
    FormatMessage(
        FORMAT_MESSAGE_FROM_SYSTEM,
        NULL,   // source
        val,
        0,      // language
        buf,
        _countof(buf),
        NULL
    );

    // trim trailing whitespace (i.e. \r\n)
    char * ptr = buf + strlen(buf) - 1;
    for (; ptr > buf && isspace(*ptr); --ptr) 
        *ptr = 0;

    os << buf;
    return os;
}
