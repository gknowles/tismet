// socket.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Incomplete public types
*
***/

class DimSocket {
};


/****************************************************************************
*
*   Private declarations
*
***/



/****************************************************************************
*
*   Variables
*
***/

static RIO_EXTENSION_FUNCTION_TABLE s_rioFuncs;

static ERunMode s_mode;


/****************************************************************************
*
*   WinSocketCleanup
*
***/

namespace {
class SocketCleanup : public IDimAppShutdownNotify {
    bool OnAppQueryConsoleDestroy () override;
};
static SocketCleanup s_cleanup;
} // namespace

//===========================================================================
bool SocketCleanup::OnAppQueryConsoleDestroy () {
    s_mode = MODE_STOPPING;
    if (WSACleanup()) 
        DimErrorLog{kError} << "WSACleanup failed, " << WSAGetLastError();
    s_mode = MODE_STOPPED;
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimSocketInitialize () {
    s_mode = MODE_STARTING;
    DimAppMonitorShutdown(&s_cleanup);
    WSADATA data = {};
    int error = WSAStartup(WINSOCK_VERSION, &data);
    if (error || data.wVersion != WINSOCK_VERSION) {
        DimErrorLog{kFatal} << "WSAStartup failed, " << error
            << "version " << hex << data.wVersion;
    }

    SOCKET s = WSASocketW(
        AF_UNSPEC, 
        SOCK_STREAM, 
        IPPROTO_TCP,
        NULL,   // protocol info (additional creation options)
        0,      // socket group
        WSA_FLAG_REGISTERED_IO
    );
    if (s == INVALID_SOCKET) 
        DimErrorLog{kFatal} << "socket failed, " << WSAGetLastError();

    // get RIO functions
    GUID extId = WSAID_MULTIPLE_RIO;
    s_rioFuncs.cbSize = sizeof(s_rioFuncs);
    DWORD bytes;
    if (WSAIoctl(
        s,
        SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &s_rioFuncs, sizeof(s_rioFuncs),
        &bytes,
        NULL,
        NULL
    )) {
        DimErrorLog{kFatal} << "WSAIoctl get RIO extension failed, " 
            << WSAGetLastError();
    }

    closesocket(s);
    s_mode = MODE_RUNNING;
}


/****************************************************************************
*
*   Public API
*
***/

void DimSocketConnect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
);
void DimSocketDisconnect (IDimSocketNotify * notify);
void DimSocketWrite (IDimSocketNotify * notify, void * data, size_t bytes);
