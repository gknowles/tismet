// socket.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/

const int kInitialCompletionQueueSize = 100;
const int kInitialSendQueueSize = 10;


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
    RIO_BUF m_rbuf = {};
    unique_ptr<DimSocketBuffer> m_buffer;

    // filled in after completion
    int m_xferStatus{0};
    int m_xferError{0};
    int m_xferBytes{0};
    DimSocket * m_socket{nullptr};
};

class ReadTask : public RequestTaskBase {
    void OnTask () override;
};

class WriteTask : public RequestTaskBase {
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
    static RunMode GetMode (IDimSocketNotify * notify);
    static void Connect (
        IDimSocketNotify * notify,
        const SockAddr & remoteAddr,
        const SockAddr & localAddr
    );
    static void Disconnect (IDimSocketNotify * notify);
    static void Write (
        IDimSocketNotify * notify, 
        unique_ptr<DimSocketBuffer> buffer,
        size_t bytes
    );

public:
    DimSocket (IDimSocketNotify * notify);
    ~DimSocket ();

    void HardClose ();

    void OnConnect (int error, int bytes);
    void OnRead ();
    void OnWrite (WriteTask * task);

    void QueueRead_LK ();
    void QueueWrite_LK (
        unique_ptr<DimSocketBuffer> buffer,
        size_t bytes
    );
    void QueueWriteFromUnsent_LK ();

private:
    IDimSocketNotify * m_notify{nullptr};
    SOCKET m_handle{INVALID_SOCKET};
    RIO_RQ m_rq{};
    
    // has received disconnect and is waiting for writes to complete
    bool m_closing{false};

    // used by single read request
    ReadTask m_read;
    static const int kMaxReceiving{1};

    // used by write requests
    list<WriteTask> m_sending;
    int m_numSending{0};
    int m_maxSending{0};
    list<WriteTask> m_unsent;
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
static int s_cqSize = 10; // kInitialCompletionQueueSize;
static int s_cqUsed;

static atomic_int s_numSockets;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void AddCqUsed_LK (int delta) {
    s_cqUsed += delta;
    assert(s_cqUsed >= 0);

    int size = s_cqSize;
    if (s_cqUsed > s_cqSize) {
        size = max(s_cqSize * 3 / 2, s_cqUsed);
    } else if (s_cqUsed < s_cqSize / 3) {
        size = max(s_cqSize / 2, kInitialCompletionQueueSize);
    }
    if (size != s_cqSize) {
        if (!s_rio.RIOResizeCompletionQueue(s_cq, size)) {
            int error = GetLastError();
            DimLog{kError} << "RIOResizeCompletionQueue(" 
                << size << ") failed, " << error;
        } else {
            s_cqSize = size;
        }
    }
}

//===========================================================================
static int HResultFromNtStatus (int ntstatus) {
    int error{0};
    if (ntstatus) {
        OVERLAPPED overlapped{};
        overlapped.Internal = ntstatus;
        DWORD bytes;
        if (!GetOverlappedResult(NULL, &overlapped, &bytes, false)) {
            error = GetLastError();
        }
    }
    return error;
}


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
            DimLog{kCrash} << "RIODequeueCompletion failed, " 
                << GetLastError();
        }

        for (int i = 0; i < count; ++i) {
            auto&& rr = results[i];
            auto task = (RequestTaskBase *) rr.RequestContext;
            task->m_socket = (DimSocket *) rr.SocketContext;
            task->m_xferStatus = rr.Status;
            task->m_xferBytes = rr.BytesTransferred;
            tasks[i] = task;
        }

        if (int error = s_rio.RIONotify(s_cq)) 
            DimLog{kCrash} << "RIONotify failed, " << error;

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
*   ReadTask
*
***/

//===========================================================================
void ReadTask::OnTask () {
    m_xferError = HResultFromNtStatus(m_xferStatus);
    m_socket->OnRead();
    // task object is a member of DimSocket and will be deleted when the 
    // socket is deleted
}


/****************************************************************************
*
*   WriteTask
*
***/

//===========================================================================
void WriteTask::OnTask () {
    m_xferError = HResultFromNtStatus(m_xferStatus);
    m_socket->OnWrite(this);
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
RunMode DimSocket::GetMode (IDimSocketNotify * notify) {
    unique_lock<mutex> lk{s_mut};
    return notify->m_socket ? kRunRunning : kRunStopped;
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
        DimLog{kError} << "WSASocket: " << WinError{};
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
            DimLog{kError} << "setsockopt(SO_PORT_SCALABILITY): " << WinError{};
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
        DimLog{kError} << "bind(" << localAddr << "): " << WinError{};
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
        DimLog{kError} << "WSAIoctl(get ConnectEx): " << WinError{};
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
            DimLog{kError} << "ConnectEx failed, " << error;
            return PushConnectFailed(notify);
        }
    }
    task.release();
}

//===========================================================================
// static 
void DimSocket::Disconnect (IDimSocketNotify * notify) {
    unique_lock<mutex> lk{s_mut};
    if (notify->m_socket)
        notify->m_socket->HardClose();
}

//===========================================================================
// static 
void DimSocket::Write (
    IDimSocketNotify * notify, 
    unique_ptr<DimSocketBuffer> buffer,
    size_t bytes
) {
    assert(bytes <= buffer->len);
    unique_lock<mutex> lk{s_mut};
    DimSocket * sock = notify->m_socket;
    if (!sock)
        return;

    sock->QueueWrite_LK(move(buffer), bytes);
}

//===========================================================================
DimSocket::DimSocket (IDimSocketNotify * notify)
    : m_notify(notify)
{
    s_numSockets += 1;
}

//===========================================================================
DimSocket::~DimSocket () {
    lock_guard<mutex> lk{s_mut};
    if (m_notify)
        m_notify->m_socket = nullptr;

    HardClose();

    if (m_maxSending)
        AddCqUsed_LK(-(m_maxSending + kMaxReceiving));

    s_numSockets -= 1;
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
    if (error) {
        m_notify->OnSocketConnectFailed();
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
        DimLog{kError} 
            << "setsockopt(SO_UPDATE_CONNECT_CONTEXT) failed, "
            << GetLastError();
        m_notify->OnSocketConnectFailed();
        delete this;
        return;
    }

    m_read.m_buffer = DimSocketGetBuffer();
    IDimSocketGetRioBuffer(
        &m_read.m_rbuf, 
        m_read.m_buffer.get(), 
        m_read.m_buffer->len
    );

    {
        unique_lock<mutex> lk{s_mut};

        // adjust size of completion queue if required
        m_maxSending = kInitialSendQueueSize;
        AddCqUsed_LK(m_maxSending + kMaxReceiving);

        // create request queue
        m_rq = s_rio.RIOCreateRequestQueue(
            m_handle,
            kMaxReceiving,  // max outstanding recv requests
            1,              // max recv buffers (must be 1)
            m_maxSending,   // max outstanding send requests
            1,              // max send buffers (must be 1)
            s_cq,           // recv completion queue
            s_cq,           // send completion queue
            this            // socket context
        );
        if (m_rq == RIO_INVALID_RQ) {
            DimLog{kError} << "RIOCreateRequestQueue failed, "
                << GetLastError();
            lk.unlock();
            m_notify->OnSocketConnectFailed();
            delete this;
            return;
        }

        // start reading from socket
        QueueRead_LK();
    }

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
        DimLog{kError} << "getpeername failed, " << GetLastError();
        m_notify->OnSocketConnectFailed();
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
        DimLog{kError} << "getsockname failed, " << GetLastError();
        m_notify->OnSocketConnectFailed();
        delete this;
        return;
    }
    DimAddressFromStorage(&info.localAddr, sas);

    m_notify->m_socket = this;
    m_notify->OnSocketConnect(info);
}

//===========================================================================
void DimSocket::OnRead () {
    if (m_read.m_xferBytes) {
        DimSocketData data;
        data.data = (char *) m_read.m_buffer->data;
        data.bytes = m_read.m_xferBytes;
        m_notify->OnSocketRead(data);

        lock_guard<mutex> lk{s_mut};
        QueueRead_LK();
    } else {
        m_notify->OnSocketDisconnect();
        unique_lock<mutex> lk{s_mut};
        if (m_sending.empty()) {
            lk.unlock();
            delete this;
        } else {
            m_closing = true;
        }
    }
}

//===========================================================================
void DimSocket::QueueRead_LK () {
    if (!s_rio.RIOReceive(
        m_rq,
        &m_read.m_rbuf,
        1,      // number of RIO_BUFs (must be 1)
        0,      // RIO_MSG_* flags
        &m_read
    )) {
        int error = GetLastError();
        DimLog{kCrash} << "RIOReceive failed, " << error;
    }
}

//===========================================================================
void DimSocket::OnWrite (WriteTask * task) {
    unique_lock<mutex> lk{s_mut};

    auto it = find_if(
        m_sending.begin(), 
        m_sending.end(), 
        [task](auto&& val) { return &val == task; }
    );
    assert(it != m_sending.end());
    m_sending.erase(it);
    m_numSending -= 1;

    // already disconnected and this was the last unresolved write? delete
    if (m_closing && m_sending.empty()) {
        lk.unlock();
        delete this;
        return;
    }

    QueueWriteFromUnsent_LK();
}

//===========================================================================
void DimSocket::QueueWrite_LK (
    unique_ptr<DimSocketBuffer> buffer,
    size_t bytes
) {
    if (!m_unsent.empty()) {
        auto & back = m_unsent.back();
        int count = min(
            back.m_buffer->len - (int) back.m_rbuf.Length, 
            (int) bytes
        );
        if (count) {
            memcpy(
                back.m_buffer->data + back.m_rbuf.Length,
                buffer->data,
                count
            );
            back.m_rbuf.Length += count;
            bytes -= count;
            if (bytes) {
                memmove(buffer->data, buffer->data + count, bytes);
            }
        }
    }

    if (bytes) {
        m_unsent.emplace_back();
        auto & task = m_unsent.back();
        IDimSocketGetRioBuffer(&task.m_rbuf, buffer.get(), bytes);
        task.m_buffer = move(buffer);
    }

    QueueWriteFromUnsent_LK();
}

//===========================================================================
void DimSocket::QueueWriteFromUnsent_LK () {
    while (m_numSending < m_maxSending && !m_unsent.empty()) {
        m_sending.splice(m_sending.end(), m_unsent, m_unsent.begin());
        m_numSending += 1;
        auto & task = m_sending.back();
        if (!s_rio.RIOSend(m_rq, &task.m_rbuf, 1, 0, &task)) {
            DimLog{kCrash} << "RIOSend failed, " << GetLastError();
            m_sending.pop_back();
            m_numSending -= 1;
        }
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
    if (s_numSockets)
        return DimQueryDestroyFailed();

    unique_lock<mutex> lk{s_mut};
    s_mode = kRunStopping;

    // wait for dispatch thread to stop
    s_cqReady.Signal();
    while (s_mode != kRunStopped)
        s_modeCv.wait(lk);

    // close windows sockets
    s_rio.RIOCloseCompletionQueue(s_cq);
    if (WSACleanup()) 
        DimLog{kError} << "WSACleanup failed, " << GetLastError();

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

    WSADATA data = {};
    int error = WSAStartup(WINSOCK_VERSION, &data);
    if (error || data.wVersion != WINSOCK_VERSION) {
        DimLog{kCrash} << "WSAStartup failed, " << error
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
        DimLog{kCrash} << "socket failed, " << GetLastError();

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
        DimLog{kCrash} << "WSAIoctl get RIO extension failed, " 
            << GetLastError();
    }
    closesocket(s);

    // initialize buffer allocator
    IDimSocketBufferInitialize(s_rio);
    // Don't register cleanup until all dependents (aka sockbuf) have
    // registered their cleanups (aka been initialized)
    DimAppMonitorShutdown(&s_cleanup);

    // create RIO completion queue
    RIO_NOTIFICATION_COMPLETION ctype = {};
    ctype.Type = RIO_EVENT_COMPLETION;
    ctype.Event.EventHandle = s_cqReady.NativeHandle();
    ctype.Event.NotifyReset = false;
    s_cq = s_rio.RIOCreateCompletionQueue(s_cqSize, &ctype);
    if (s_cq == RIO_INVALID_CQ) {
        DimLog{kCrash} << "RIOCreateCompletionQueue, " 
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
RunMode DimSocketGetMode (IDimSocketNotify * notify) {
    return DimSocket::GetMode(notify);
}

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
void DimSocketWrite (
    IDimSocketNotify * notify, 
    unique_ptr<DimSocketBuffer> buffer,
    size_t bytes
) {
    DimSocket::Write (notify, move(buffer), bytes);
}
