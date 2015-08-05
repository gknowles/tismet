// socket.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kInitialCompletionQueueSize = 100;


/****************************************************************************
*
*   Incomplete public types
*
***/

class DimSocket {
public:
    static void Connect (
        IDimSocketNotify * notify,
        const SockAddr & remoteAddr,
        const SockAddr & localAddr
    );

private:
    SOCKET m_handle;
    RIO_RQ m_rq;
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class RioDispatchTask : public IDimTaskNotify {
    void OnTask () override;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static RioDispatchTask s_dispatchTask;
static RIO_EXTENSION_FUNCTION_TABLE s_rio;

static mutex s_mut;
static condition_variable s_modeCv; // when run mode changes to stopped
static RunMode s_mode{kRunStopped};
static WinEvent s_cqReady;
static RIO_CQ s_cq;
static int s_cqSize = kInitialCompletionQueueSize;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================


/****************************************************************************
*
*   RioDispatchTask
*
***/

//===========================================================================
void RioDispatchTask::OnTask () {
    s_rio.RIONotify(s_cq);

    for (;;) {
        s_cqReady.Wait();

        lock_guard<mutex> lk{s_mut};
        if (s_mode == kRunStopping) {
            s_mode = kRunStopped;
            break;
        }


    }

    s_modeCv.notify_one();
}


/****************************************************************************
*
*   DimSocket
*
***/

//===========================================================================
// static
void DimSocket::Connect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    DimSocket * sock = notify->m_socket;
    if (!sock) {
        sock = notify->m_socket = new DimSocket;
        sock->m_handle = WSASocketW(
            AF_UNSPEC,
            SOCK_STREAM,
            IPPROTO_TCP,
            NULL,
            0,
            WSA_FLAG_REGISTERED_IO
        );

    }

    ref(notify);
    ref(remoteAddr);
    ref(localAddr);
}


/****************************************************************************
*
*   DimSocketShutdown
*
***/

namespace {
    class DimSocketShutdown : public IDimAppShutdownNotify {
        bool OnAppQueryConsoleDestroy () override;
    };
} // namespace
static DimSocketShutdown s_cleanup;

//===========================================================================
bool DimSocketShutdown::OnAppQueryConsoleDestroy () {
    unique_lock<mutex> lk{s_mut};
    s_mode = kRunStopping;

    // wait for dispatch thread to stop
    s_cqReady.Signal();
    while (s_mode != kRunStopped)
        s_modeCv.wait(lk);

    // close windows sockets
    s_rio.RIOCloseCompletionQueue(s_cq);
    if (WSACleanup()) 
        DimErrorLog{kError} << "WSACleanup failed, " << WSAGetLastError();

    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimSocketInitialize () {
    s_mode = kRunStarting;
    DimAppMonitorShutdown(&s_cleanup);

    WSADATA data = {};
    int error = WSAStartup(WINSOCK_VERSION, &data);
    if (error || data.wVersion != WINSOCK_VERSION) {
        DimErrorLog{kFatal} << "WSAStartup failed, " << error
            << "version " << hex << data.wVersion;
    }

    // get extension functions
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
    s_rio.cbSize = sizeof(s_rio);
    DWORD bytes;
    if (WSAIoctl(
        s,
        SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &s_rio, sizeof(s_rio),
        &bytes,
        NULL,
        NULL
    )) {
        DimErrorLog{kFatal} << "WSAIoctl get RIO extension failed, " 
            << WSAGetLastError();
    }
    closesocket(s);

    // initialize buffer allocator
    IDimSocketBufferInitialize(s_rio);

    // create RIO completion queue
    RIO_NOTIFICATION_COMPLETION ctype = {};
    ctype.Type = RIO_EVENT_COMPLETION;
    ctype.Event.EventHandle = s_cqReady.NativeHandle();
    ctype.Event.NotifyReset = false;
    s_cq = s_rio.RIOCreateCompletionQueue(s_cqSize, &ctype);
    if (s_cq == RIO_INVALID_CQ) {
        DimErrorLog{kFatal} << "RIOCreateCompletionQueue, " 
            << WSAGetLastError();
    }

    // start rio dispatch task
    HDimTaskQueue taskq = DimTaskCreateQueue("RIO Dispatch", 1);
    DimTaskPush(taskq, s_dispatchTask);

    s_mode = kRunRunning;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void DimSocketConnect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    DimSocket::Connect(notify, remoteAddr, localAddr);
}

//===========================================================================
void DimSocketDisconnect (IDimSocketNotify * notify);

//===========================================================================
void DimSocketWrite (IDimSocketNotify * notify, void * data, size_t bytes);
