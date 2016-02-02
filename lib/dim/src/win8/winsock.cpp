// winsock.cpp - dim core - windows platform
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
            DimLog{kError} << "RIOResizeCompletionQueue(" 
                << size << "): " << WinError{};
        } else {
            s_cqSize = size;
        }
    }
}


/****************************************************************************
*
*   RioDispatchThread
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
    IDimTaskNotify * tasks[size(results)];
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
            (ULONG) size(results)
        );
        if (count == RIO_CORRUPT_CQ)
            DimLog{kCrash} << "RIODequeueCompletion: " << WinError{};

        for (int i = 0; i < count; ++i) {
            auto&& rr = results[i];
            auto task = (DimSocket::RequestTaskBase *) rr.RequestContext;
            task->m_socket = (DimSocket *) rr.SocketContext;
            task->m_xferError = (WinError::NtStatus) rr.Status;
            task->m_xferBytes = rr.BytesTransferred;
            tasks[i] = task;
        }

        if (int error = s_rio.RIONotify(s_cq)) 
            DimLog{kCrash} << "RIONotify: " << WinError{};

        lk.unlock();

        if (count) 
            DimTaskPushEvent(tasks, count);

        s_cqReady.Wait();
    }

    s_modeCv.notify_one();
}


/****************************************************************************
*
*   DimSocket::ReadTask
*
***/

//===========================================================================
void DimSocket::ReadTask::OnTask () {
    m_socket->OnRead();
    // task object is a member of DimSocket and will be deleted when the 
    // socket is deleted
}


/****************************************************************************
*
*   DimSocket::WriteTask
*
***/

//===========================================================================
void DimSocket::WriteTask::OnTask () {
    // deleted via containing list
    m_socket->OnWrite(this);
}


/****************************************************************************
*
*   DimSocket
*
***/

//===========================================================================
// static
DimSocket::Mode DimSocket::GetMode (IDimSocketNotify * notify) {
    unique_lock<mutex> lk{s_mut};
    if (auto * sock = notify->m_socket) 
        return sock->m_mode;

    return Mode::kInactive;
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

    m_mode = Mode::kClosing;
    m_handle = INVALID_SOCKET;
}

//===========================================================================
bool DimSocket::CreateQueue () {
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
            DimLog{kError} << "RIOCreateRequestQueue: " << WinError{};
            return false;
        }

        m_mode = Mode::kActive;
        m_notify->m_socket = this;

        // start reading from socket
        QueueRead_LK();
    }

    return true;
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
            m_mode = Mode::kClosed;
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
        DimLog{kCrash} << "RIOReceive: " << WinError{};
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
    if (m_mode == Mode::kClosed && m_sending.empty()) {
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
            DimLog{kCrash} << "RIOSend: " << WinError{};
            m_sending.pop_back();
            m_numSending -= 1;
        }
    }
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {
    class ShutdownNotify : public IDimAppShutdownNotify {
        bool OnAppQueryConsoleDestroy () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
bool ShutdownNotify::OnAppQueryConsoleDestroy () {
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
        DimLog{kError} << "WSACleanup: " << WinError{};

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
    WinError err = WSAStartup(WINSOCK_VERSION, &data);
    if (err || data.wVersion != WINSOCK_VERSION) {
        DimLog{kCrash} << "WSAStartup(version=" << hex << WINSOCK_VERSION
            << "): " << err << ", version " << data.wVersion;
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
        DimLog{kCrash} << "socket: " << WinError{};

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
        nullptr,    // overlapped
        nullptr     // completion routine
    )) {
        DimLog{kCrash} << "WSAIoctl(get RIO extension): " << WinError{};
    }
    closesocket(s);

    // initialize buffer allocator
    IDimSocketBufferInitialize(s_rio);
    // Don't register cleanup until all dependents (aka sockbuf) have
    // registered their cleanups (aka been initialized)
    DimAppMonitorShutdown(&s_cleanup);
    IDimSocketAcceptInitialize();
    IDimSocketConnectInitialize();

    // create RIO completion queue
    RIO_NOTIFICATION_COMPLETION ctype = {};
    ctype.Type = RIO_EVENT_COMPLETION;
    ctype.Event.EventHandle = s_cqReady.NativeHandle();
    ctype.Event.NotifyReset = false;
    s_cq = s_rio.RIOCreateCompletionQueue(s_cqSize, &ctype);
    if (s_cq == RIO_INVALID_CQ)
        DimLog{kCrash} << "RIOCreateCompletionQueue: " << WinError{};

    // start rio dispatch task
    HDimTaskQueue taskq = DimTaskCreateQueue("RIO Dispatch", 1);
    DimTaskPush(taskq, s_dispatchThread);

    s_mode = kRunRunning;
}


/****************************************************************************
*
*   Win socket
*
***/

//===========================================================================
SOCKET WinSocketCreate () {
    SOCKET handle = WSASocketW(
        AF_UNSPEC,
        SOCK_STREAM,
        IPPROTO_TCP,
        NULL,
        0,
        WSA_FLAG_REGISTERED_IO
    );
    if (handle == INVALID_SOCKET) {
        DimLog{kError} << "WSASocket: " << WinError{};
        return INVALID_SOCKET;
    }

    int yes = 1;

    //DWORD bytes;
    //if (SOCKET_ERROR == WSAIoctl(
    //    handle,
    //    SIO_LOOPBACK_FAST_PATH,
    //    &yes, sizeof yes,
    //    nullptr, 0, // output buffer, buffer size
    //    &bytes,     // bytes returned
    //    nullptr,    // overlapped
    //    nullptr     // completion routine
    //)) {
    //    DimLog{kError} << "WSAIoctl(SIO_LOOPBACK_FAST_PATH): " << WinError{};
    //}

    if (SOCKET_ERROR == setsockopt(
        handle,
        SOL_SOCKET,
        TCP_NODELAY,
        (char *) &yes,
        sizeof(yes)
    )) {
        DimLog{kError} << "WSAIoctl(FIONBIO): " << WinError{};
    }

#ifdef SO_REUSE_UNICASTPORT
    if (SOCKET_ERROR == setsockopt(
        handle,
        SOL_SOCKET,
        SO_REUSE_UNICASTPORT,
        (char *) &yes,
        sizeof(yes)
    )) {
#endif        
        if (SOCKET_ERROR == setsockopt(
            handle, 
            SOL_SOCKET, 
            SO_PORT_SCALABILITY, 
            (char *) &yes, 
            sizeof(yes)
        )) {
            DimLog{kError} << "setsockopt(SO_PORT_SCALABILITY): " 
                << WinError{};
        }
#ifdef SO_REUSE_UNICASTPORT
    }
#endif

    return handle;
}

//===========================================================================
SOCKET WinSocketCreate (const Endpoint & end) {
    SOCKET handle = WinSocketCreate();
    if (handle == INVALID_SOCKET) 
        return handle;

    sockaddr_storage sas;
    DimEndpointToStorage(&sas, end);
    if (SOCKET_ERROR == ::bind(
        handle, 
        (sockaddr *) &sas, 
        sizeof(sas)
    )) {
        DimLog{kError} << "bind(" << end << "): " << WinError{};
        closesocket(handle);
        return INVALID_SOCKET;
    }

    return handle;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
IDimSocketNotify::Mode DimSocketGetMode (IDimSocketNotify * notify) {
    return DimSocket::GetMode(notify);
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
