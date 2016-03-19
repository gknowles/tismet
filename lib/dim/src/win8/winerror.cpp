// winerror.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private declarations
*
***/

using RtlNtStatusToDosErrorFn = ULONG (WINAPI *)(int ntStatus);


/****************************************************************************
*
*   Variables
*
***/

static RtlNtStatusToDosErrorFn s_RtlNtStatusToDosError;
static once_flag s_loadOnce;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void loadProc () {
    HMODULE mod = LoadLibrary("ntdll.dll");
    if (!mod)
        logMsgCrash() << "LoadLibrary(ntdll): " << WinError{};

    s_RtlNtStatusToDosError = (RtlNtStatusToDosErrorFn) GetProcAddress(
        mod,
        "RtlNtStatusToDosError"
    );
    if (!s_RtlNtStatusToDosError) {
        logMsgCrash() << "GetProcAddress(RtlNtStatusToDosError): " 
            << WinError{};
    }
}


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
WinError::WinError (int error) 
    : m_value{error}
{}

//===========================================================================
WinError::WinError (NtStatus status) {
    operator=(status);
}

//===========================================================================
WinError & WinError::operator= (int error) {
    m_value = error;
    return *this;
}

//===========================================================================
WinError & WinError::operator= (NtStatus status) {
    if (!status) {
        m_value = 0;
    } else {
        call_once(s_loadOnce, loadProc);
        m_value = s_RtlNtStatusToDosError(status);
    }
    return *this;
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
        (DWORD) size(buf),
        NULL
    );

    // trim trailing whitespace (i.e. \r\n)
    char * ptr = buf + strlen(buf) - 1;
    for (; ptr > buf && isspace((unsigned char) *ptr); --ptr) 
        *ptr = 0;

    os << buf;
    return os;
}

} // namespace
