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
*   Private declarations
*
***/

namespace {

class ConnectTask : public IWinEventWaitNotify {
public:
    unique_ptr<DimSocket> m_socket;
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

class RequestTaskBase : public IDimTaskNotify {
    virtual void OnTask () override = 0;
public:
    // filled in by the dispatch thread
    int m_ntstatus = 0;
    int m_bytes = 0;
    DimSocket * m_socket = nullptr;
};

class ReceiveTask : public RequestTaskBase {
    void OnTask () override;
};

} // namespace


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
    static void Disconnect (IDimSocketNotify * notify);

public:
    DimSocket (IDimSocketNotify * notify);
    ~DimSocket ();

    void HardClose ();

    void OnConnect (int error, int bytes);
    void OnReceive ();

    void QueueReceive ();

private:
    IDimSocketNotify * m_notify;
    SOCKET m_handle = INVALID_SOCKET;
    RIO_RQ m_rq = {};
    RIO_BUF m_rbuf = {};
    unique_ptr<DimSocketBuffer> m_buffer;
    ReceiveTask m_recv;
};


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
                << GetLastError();
        }

        for (int i = 0; i < count; ++i) {
            auto&& rr = results[i];
            auto task = (RequestTaskBase *) rr.RequestContext;
            task->m_socket = (DimSocket *) rr.SocketContext;
            task->m_ntstatus = rr.Status;
            task->m_bytes = rr.BytesTransferred;
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
    : m_socket(move(sock))
{}

//===========================================================================
void ConnectTask::OnTask () {
    DWORD bytesTransferred;
    int error{0};
    if (!GetOverlappedResult(
        NULL, 
        &m_overlapped, 
        &bytesTransferred, 
        false   // wait?
    )) {
        error = GetLastError();
    }
    m_socket->OnConnect(error, bytesTransferred);
    m_socket.release();
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
*   ReceiveTask
*
***/

//===========================================================================
void ReceiveTask::OnTask () {
    m_socket->OnReceive();
    // task object is a member of DimSocket and will be deleted when the 
    // socket is deleted
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
        int error = GetLastError();
        DimErrorLog{kError} << "WSASocket failed, " << error;
        return PushConnectFailed(notify);
    }

    // TODO: SIO_LOOPBACK_FAST_PATH

    int yes = 1;
#ifdef SO_REUSE_UNICASTPORT
    if (SOCKET_ERROR == setsockopt(
        sock->m_handle,
        SOL_SOCKET,
        SO_REUSE_UNICASTPORT,
        (char *) &yes,
        sizeof(yes)
    )) {
#endif        
        if (SOCKET_ERROR == setsockopt(
            sock->m_handle, 
            SOL_SOCKET, 
            SO_PORT_SCALABILITY, 
            (char *) &yes, 
            sizeof(yes)
        )) {
            DimErrorLog{kError} << "setsockopt(SO_PORT_SCALABILITY) failed, "
                << GetLastError();
        }
#ifdef SO_REUSE_UNICASTPORT
    }
#endif

    sockaddr_storage sas;
    DimAddressToStorage(&sas, localAddr);
    if (SOCKET_ERROR == ::bind(
        sock->m_handle, 
        (sockaddr *) &sas, 
        sizeof(sas)
    )) {
        DimErrorLog{kError} << "bind failed, " << GetLastError();
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
            << GetLastError();
        return PushConnectFailed(notify);
    }

    DimAddressToStorage(&sas, remoteAddr);
    auto task = make_unique<ConnectTask>(move(sock));
    if (!fConnectEx(
        task->m_socket->m_handle,
        (sockaddr *) &sas,
        sizeof(sas),
        NULL,   // send buffer
        0,      // send buffer length
        NULL,   // bytes sent
        &task->m_overlapped
    )) {
        int error = GetLastError();
        if (error != ERROR_IO_PENDING) {
            DimErrorLog{kError} << "ConnectEx failed, " << error;
            return PushConnectFailed(notify);
        }
    }
    task.release();
}

//===========================================================================
// static 
void DimSocket::Disconnect (IDimSocketNotify * notify) {
    if (notify->m_socket)
        notify->m_socket->HardClose();
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
    if (m_handle == INVALID_SOCKET)
        return;

    linger opt = {};
    setsockopt(m_handle, SOL_SOCKET, SO_LINGER, (char *) &opt, sizeof(opt));
    closesocket(m_handle);
    m_handle = INVALID_SOCKET;
}

//===========================================================================
void DimSocket::OnConnect (
    int error,
    int bytes
) {
    auto notify = m_notify;

    if (error) {
        notify->OnSocketConnectFailed();
        delete this;
        return;
    }

    //-----------------------------------------------------------------------
    // update socket and start receiving
    if (SOCKET_ERROR == setsockopt(
        m_handle, 
        SOL_SOCKET,
        SO_UPDATE_CONNECT_CONTEXT,
        NULL,
        0
    )) {
        DimErrorLog{kError} 
            << "setsockopt(SO_UPDATE_CONNECT_CONTEXT) failed, "
            << GetLastError();
        notify->OnSocketConnectFailed();
        delete this;
        return;
    }

    // create request queue
    m_rq = s_rio.RIOCreateRequestQueue(
        m_handle,
        1,      // max outstanding recv requests
        1,      // max recv buffers (must be 1)
        10,     // max outstanding send requests
        1,      // max send buffers (must be 1)
        s_cq,   // recv completion queue
        s_cq,   // send completion queue
        this    // socket context
    );
    if (m_rq == RIO_INVALID_RQ) {
        DimErrorLog{kError} << "RIOCreateRequestQueue failed, "
            << GetLastError();
        notify->OnSocketConnectFailed();
        delete this;
        return;
    }

    m_buffer = DimSocketGetBuffer();
    IDimSocketGetRioBuffer(&m_rbuf, m_buffer.get());

    // start reading from socket
    QueueReceive();

    //-----------------------------------------------------------------------
    // notify socket connect event
    DimSocketConnectInfo info = {};
    sockaddr_storage sas = {};

    // TODO: use getsockopt(SO_BSP_STATE) instead of getpeername & getsockname
    // address of remote node
    int sasLen = sizeof(sas);
    if (SOCKET_ERROR == getpeername(
        m_handle, 
        (sockaddr *) &sas, 
        &sasLen
    )) {
        DimErrorLog{kError} << "getpeername failed, " << GetLastError();
        notify->OnSocketConnectFailed();
        delete this;
        return;
    }
    DimAddressFromStorage(&info.remoteAddr, sas);

    // locally bound address
    if (SOCKET_ERROR == getsockname(
        m_handle, 
        (sockaddr *) &sas, 
        &sasLen
    )) {
        DimErrorLog{kError} << "getsockname failed, " << GetLastError();
        notify->OnSocketConnectFailed();
        delete this;
        return;
    }
    DimAddressFromStorage(&info.localAddr, sas);

    m_notify->OnSocketConnect(info);
}

//===========================================================================
void DimSocket::OnReceive () {
    if (m_recv.m_bytes) {
        DimSocketData data;
        data.data = (char *) m_buffer->data;
        data.bytes = m_recv.m_bytes;
        m_notify->OnSocketRead(data);
        QueueReceive();
    } else {
        m_notify->OnSocketDisconnect();
        delete this;
    }
}

//===========================================================================
void DimSocket::QueueReceive () {
    unique_lock<mutex> lk{s_mut};
    if (!s_rio.RIOReceive(
        m_rq,
        &m_rbuf,
        1,      // number of RIO_BUFs (must be 1)
        0,      // RIO_MSG_* flags
        &m_recv
    )) {
        int error = GetLastError();
        DimErrorLog{kFatal} << "RIOReceive failed, " << error;
    }
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
        DimErrorLog{kError} << "WSACleanup failed, " << GetLastError();

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
        DimErrorLog{kFatal} << "socket failed, " << GetLastError();

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
            << GetLastError();
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
            << GetLastError();
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
void DimSocketDisconnect (IDimSocketNotify * notify) {
    DimSocket::Disconnect(notify);
}

//===========================================================================
void DimSocketWrite (IDimSocketNotify * notify, void * data, size_t bytes);
