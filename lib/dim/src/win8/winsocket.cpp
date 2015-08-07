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

public:
    DimSocket (IDimSocketNotify * notify);
    ~DimSocket ();

    void HardClose ();

    void OnConnect ();

private:
    IDimSocketNotify * m_notify;
    SOCKET m_handle = INVALID_SOCKET;
    RIO_RQ m_rq = {};
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class ConnectTask : public IWinEventWaitNotify {
public:
    unique_ptr<DimSocket> m_sock;
public:
    ConnectTask (unique_ptr<DimSocket> && sock);
    void OnTask () override;
};

class ConnectFailedTask : public IDimTaskNotify {
    IDimSocketNotify * m_notify;
public:
    ConnectFailedTask (IDimSocketNotify * notify);
    void OnTask () override;
};

class RioRequestTask : public IDimTaskNotify {
    void OnTask () override;

public:
    int ntstatus;
    int bytes;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

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

namespace {
    class RioDispatchThread : public IDimTaskNotify {
        void OnTask () override;
    };
}
static RioDispatchThread s_dispatchThread;

//===========================================================================
void RioDispatchThread::OnTask () {
    static const int kNumResults = 100;
    RIORESULT results[kNumResults];
    IDimTaskNotify * tasks[_countof(results)];
    int count;

    for (;;) {
        unique_lock<mutex> lk{s_mut};
        if (s_mode == kRunStopping) {
            s_mode = kRunStopped;
            break;
        }

        count = s_rio.RIODequeueCompletion(
            s_cq,
            results,
            _countof(results)
        );
        if (count == RIO_CORRUPT_CQ) {
            DimErrorLog{kFatal} << "RIODequeueCompletion failed, " 
                << WSAGetLastError();
        }

        for (int i = 0; i < count; ++i) {
            auto&& rr = results[i];
            auto task = (RioRequestTask *) rr.SocketContext;
            task->ntstatus = rr.Status;
            task->bytes = rr.BytesTransferred;
            tasks[i] = task;
        }

        if (int error = s_rio.RIONotify(s_cq)) 
            DimErrorLog{kFatal} << "RIONotify failed, " << error;

        lk.unlock();

        if (count) 
            DimTaskPushEvent(tasks, count);

        s_cqReady.Wait();
    }

    s_modeCv.notify_one();
}


/****************************************************************************
*
*   ConnectTask
*
***/

//===========================================================================
ConnectTask::ConnectTask (unique_ptr<DimSocket> && sock) 
    : m_sock(move(sock))
{}

//===========================================================================
void ConnectTask::OnTask () {
    m_sock->OnConnect();
    m_sock.release();
    delete this;
}


/****************************************************************************
*
*   ConnectFailedTask
*
***/

//===========================================================================
ConnectFailedTask::ConnectFailedTask (IDimSocketNotify * notify)
    : m_notify(notify)
{}

//===========================================================================
void ConnectFailedTask::OnTask () {
    m_notify->OnSocketConnectFailed();
    delete this;
}


/****************************************************************************
*
*   DimSocket
*
***/

//===========================================================================
static void PushConnectFailed (IDimSocketNotify * notify) {
    auto ptr = new ConnectFailedTask(notify);
    DimTaskPushEvent(*ptr);
}

//===========================================================================
// static
void DimSocket::Connect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    assert(!notify->m_socket);
    auto sock = make_unique<DimSocket>(notify);
    sock->m_handle = WSASocketW(
        AF_UNSPEC,
        SOCK_STREAM,
        IPPROTO_TCP,
        NULL,
        0,
        WSA_FLAG_REGISTERED_IO
    );
    if (sock->m_handle == INVALID_SOCKET) {
        int error = WSAGetLastError();
        DimErrorLog{kError} << "WSASocket failed, " << error;
        return PushConnectFailed(notify);
    }

    int yes = 1;
    setsockopt(
        sock->m_handle, 
        SOL_SOCKET, 
        SO_PORT_SCALABILITY, 
        (char *) &yes, 
        sizeof(yes)
    );

    sockaddr_storage sas;
    DimAddressToStorage(&sas, localAddr);
    if (SOCKET_ERROR == ::bind(
        sock->m_handle, 
        (sockaddr *) &sas, 
        sizeof(sas)
    )) {
        DimErrorLog{kError} << "bind failed, " << WSAGetLastError();
        return PushConnectFailed(notify);
    }

    // get ConnectEx function
    GUID extId = WSAID_CONNECTEX;
    LPFN_CONNECTEX fConnectEx;
    DWORD bytes;
    if (WSAIoctl(
        sock->m_handle,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &fConnectEx, sizeof(fConnectEx),
        &bytes,
        NULL,
        NULL
    )) {
        DimErrorLog{kError} << "WSAIoctl get ConnectEx failed, " 
            << WSAGetLastError();
        return PushConnectFailed(notify);
    }

    DimAddressToStorage(&sas, remoteAddr);
    auto task = make_unique<ConnectTask>(move(sock));
    if (!fConnectEx(
        sock->m_handle,
        (sockaddr *) &sas,
        sizeof(sas),
        NULL,   // send buffer
        0,      // send buffer length
        NULL,   // bytes sent
        &task->m_overlapped
    )) {
        int error = WSAGetLastError();
        if (error != ERROR_IO_PENDING) {
            DimErrorLog{kError} << "ConnectEx failed, " << error;
            return PushConnectFailed(notify);
        }
    }
    task.release();
}

//===========================================================================
DimSocket::DimSocket (IDimSocketNotify * notify)
    : m_notify(notify)
{}

//===========================================================================
DimSocket::~DimSocket () {
    if (m_notify)
        m_notify->m_socket = nullptr;

    HardClose();
}

//===========================================================================
void DimSocket::HardClose () {
    if (!m_handle)
        return;

    linger opt = {};
    setsockopt(m_handle, SOL_SOCKET, SO_LINGER, (char *) &opt, sizeof(opt));
    closesocket(m_handle);
}

//===========================================================================
void DimSocket::OnConnect () {
    auto notify = m_notify;

    if (SOCKET_ERROR == setsockopt(
        m_handle, 
        SOL_SOCKET,
        SO_UPDATE_CONNECT_CONTEXT,
        NULL,
        0
    )) {
        DimErrorLog{kError} 
            << "setsockopt(SO_UPDATE_CONNECT_CONTEXT) failed, "
            << WSAGetLastError();
        delete this;
        notify->OnSocketConnectFailed();
    }

    // TODO: RIOCreateRequestQueue
    DimSocketConnectInfo info = {};
    // TODO: getpeername
    m_notify->OnSocketConnect(info);
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
    DimTaskPush(taskq, s_dispatchThread);

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
