// winsockconn.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/

const Duration kConnectTimeout{10s};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class ConnSocket : public DimSocket {
public:
    static void Connect (
        IDimSocketNotify * notify,
        const SockAddr & remoteAddr,
        const SockAddr & localAddr,
        Duration timeout
    );
public:
    using DimSocket::DimSocket;
    void OnConnect (int error, int bytes);
};

class ConnectTask : public IWinEventWaitNotify {
public:
    TimePoint m_expiration;
    unique_ptr<ConnSocket> m_socket;
    list<ConnectTask>::iterator m_iter;
public:
    ConnectTask (unique_ptr<ConnSocket> && sock);
    void OnTask () override;
};

class ConnectFailedTask : public IDimTaskNotify {
    IDimSocketNotify * m_notify{nullptr};
public:
    ConnectFailedTask (IDimSocketNotify * notify);
    void OnTask () override;
};

class ConnectTimer : public IDimTimerNotify {
    Duration OnTimer () override;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static mutex s_mut;
static list<ConnectTask> s_connecting;
static list<ConnectTask> s_closing;
static ConnectTimer s_connectTimer;


/****************************************************************************
*
*   ConnectTimer
*
***/

//===========================================================================
Duration ConnectTimer::OnTimer () {
    TimePoint now{DimClock::now()};
    lock_guard<mutex> lk{s_mut};
    while (!s_connecting.empty()) {
        auto it = s_connecting.begin();
        if (now < it->m_expiration) {
            return it->m_expiration - now;
        }
        it->m_socket->HardClose();
        it->m_expiration = TimePoint::max();
        s_closing.splice(s_closing.end(), s_connecting, it);
    }
    return DIM_TIMER_INFINITE;
}


/****************************************************************************
*
*   ConnectTask
*
***/

//===========================================================================
ConnectTask::ConnectTask (unique_ptr<ConnSocket> && sock) 
    : m_socket(move(sock))
{
    m_expiration = DimClock::now() + kConnectTimeout;
}

//===========================================================================
void ConnectTask::OnTask () {
    DWORD bytesTransferred;
    WinError err{0};
    if (!GetOverlappedResult(
        NULL, 
        &m_overlapped, 
        &bytesTransferred, 
        false   // wait?
    )) {
        err = WinError{};
    }
    m_socket.release()->OnConnect(err, bytesTransferred);

    lock_guard<mutex> lk{s_mut};
    if (m_expiration == TimePoint::max()) {
        s_closing.erase(m_iter);
    } else {
        s_connecting.erase(m_iter);
    }
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
*   ConnSocket
*
***/

//===========================================================================
static void PushConnectFailed (IDimSocketNotify * notify) {
    auto ptr = new ConnectFailedTask(notify);
    DimTaskPushEvent(*ptr);
}

//===========================================================================
// static
void ConnSocket::Connect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr,
    Duration timeout
) {
    assert(GetMode(notify) == Mode::kInactive);

    if (timeout == 0ms)
        timeout = kConnectTimeout;

    auto sock = make_unique<ConnSocket>(notify);
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

    sock->m_mode = Mode::kConnecting;
    list<ConnectTask>::iterator it;
    DimTimerUpdate(&s_connectTimer, timeout, true);

    {
        lock_guard<mutex> lk{s_mut};
        TimePoint expiration = DimClock::now() + timeout;
        
        // TODO: check if this really puts them in expiration order!
        auto rhint = find_if(
            s_connecting.rbegin(), 
            s_connecting.rend(),
            [&](auto&& task){ return task.m_expiration <= expiration; }
        );
        it = s_connecting.emplace(rhint.base(), move(sock));

        it->m_iter = it;
        it->m_expiration = expiration;
    }

    DimAddressToStorage(&sas, remoteAddr);
    bool error = !fConnectEx(
        it->m_socket->m_handle,
        (sockaddr *) &sas,
        sizeof(sas),
        NULL,   // send buffer
        0,      // send buffer length
        NULL,   // bytes sent
        &it->m_overlapped
    );
    WinError err;
    if (!error || err != ERROR_IO_PENDING) {
        DimLog{kError} << "ConnectEx(" << remoteAddr << "): " << err;
        lock_guard<mutex> lk{s_mut};
        s_connecting.pop_back();
        return PushConnectFailed(notify);
    }
}

//===========================================================================
void ConnSocket::OnConnect (
    int error,
    int bytes
) {
    unique_ptr<ConnSocket> hostage(this);

    if (m_mode == IDimSocketNotify::kClosing) 
        return m_notify->OnSocketConnectFailed();

    assert(m_mode == IDimSocketNotify::kConnecting);

    if (error)
        return m_notify->OnSocketConnectFailed();

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
            << "setsockopt(SO_UPDATE_CONNECT_CONTEXT): " 
            << WinError{};
        return m_notify->OnSocketConnectFailed();
    }

    //-----------------------------------------------------------------------
    // get local and remote addresses
    sockaddr_storage sas = {};

    // TODO: use getsockopt(SO_BSP_STATE) instead of getpeername & getsockname

    // address of remote node
    int sasLen = sizeof(sas);
    if (SOCKET_ERROR == getpeername(
        m_handle, 
        (sockaddr *) &sas, 
        &sasLen
    )) {
        DimLog{kError} << "getpeername: " << WinError{};
        return m_notify->OnSocketConnectFailed();
    }
    DimAddressFromStorage(&m_connInfo.remoteAddr, sas);

    // locally bound address
    if (SOCKET_ERROR == getsockname(
        m_handle, 
        (sockaddr *) &sas, 
        &sasLen
    )) {
        DimLog{kError} << "getsockname: " << WinError{};
        return m_notify->OnSocketConnectFailed();
    }
    DimAddressFromStorage(&m_connInfo.localAddr, sas);

    //-----------------------------------------------------------------------
    // create read/write queue
    if (!CreateQueue()) 
        return m_notify->OnSocketConnectFailed();

    // notify socket connect event
    hostage.release();
    m_notify->OnSocketConnect(m_connInfo);
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {
    class ShutdownNotify : public IDimAppShutdownNotify {
        void OnAppStartConsoleCleanup () override;
        bool OnAppQueryConsoleDestroy () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::OnAppStartConsoleCleanup () {
    lock_guard<mutex> lk{s_mut};
    for (auto&& task : s_connecting)
        task.m_socket->HardClose();
}

//===========================================================================
bool ShutdownNotify::OnAppQueryConsoleDestroy () {
    lock_guard<mutex> lk{s_mut};
    return s_connecting.empty()
        && s_closing.empty();
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimSocketConnectInitialize () {
    // Don't register cleanup until all dependents (aka sockbuf) have
    // registered their cleanups (aka been initialized)
    DimAppMonitorShutdown(&s_cleanup);

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
    const SockAddr & localAddr,
    Duration timeout
) {
    ConnSocket::Connect(notify, remoteAddr, localAddr, timeout);
}
