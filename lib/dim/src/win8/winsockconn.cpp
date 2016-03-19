// winsockconn.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


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

class ConnSocket : public SocketBase {
public:
    static void connect (
        ISocketNotify * notify,
        const Endpoint & remoteEnd,
        const Endpoint & localEnd,
        Duration timeout
    );
public:
    using SocketBase::SocketBase;
    void onConnect (int error, int bytes);
};

class ConnectTask : public IWinEventWaitNotify {
public:
    TimePoint m_expiration;
    unique_ptr<ConnSocket> m_socket;
    list<ConnectTask>::iterator m_iter;
public:
    ConnectTask (unique_ptr<ConnSocket> && sock);
    void onTask () override;
};

class ConnectFailedTask : public ITaskNotify {
    ISocketNotify * m_notify{nullptr};
public:
    ConnectFailedTask (ISocketNotify * notify);
    void onTask () override;
};

class ConnectTimer : public ITimerNotify {
    Duration onTimer (TimePoint now) override;
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
Duration ConnectTimer::onTimer (TimePoint now) {
    lock_guard<mutex> lk{s_mut};
    while (!s_connecting.empty()) {
        auto it = s_connecting.begin();
        if (now < it->m_expiration) {
            return it->m_expiration - now;
        }
        it->m_socket->hardClose();
        it->m_expiration = TimePoint::max();
        s_closing.splice(s_closing.end(), s_connecting, it);
    }
    return kTimerInfinite;
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
    m_expiration = Clock::now() + kConnectTimeout;
}

//===========================================================================
void ConnectTask::onTask () {
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
    m_socket.release()->onConnect(err, bytesTransferred);

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
ConnectFailedTask::ConnectFailedTask (ISocketNotify * notify)
    : m_notify(notify)
{}

//===========================================================================
void ConnectFailedTask::onTask () {
    m_notify->onSocketConnectFailed();
    delete this;
}


/****************************************************************************
*
*   ConnSocket
*
***/

//===========================================================================
static void pushConnectFailed (ISocketNotify * notify) {
    auto ptr = new ConnectFailedTask(notify);
    taskPushEvent(*ptr);
}

//===========================================================================
// static
void ConnSocket::connect (
    ISocketNotify * notify,
    const Endpoint & remoteEnd,
    const Endpoint & localEnd,
    Duration timeout
) {
    assert(getMode(notify) == Mode::kInactive);

    if (timeout == 0ms)
        timeout = kConnectTimeout;

    auto sock = make_unique<ConnSocket>(notify);
    sock->m_handle = winSocketCreate(localEnd);
    if (sock->m_handle == INVALID_SOCKET) 
        return pushConnectFailed(notify);

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
        nullptr,    // overlapped
        nullptr     // completion routine
    )) {
        logMsgError() << "WSAIoctl(get ConnectEx): " << WinError{};
        return pushConnectFailed(notify);
    }

    sock->m_mode = Mode::kConnecting;
    list<ConnectTask>::iterator it;
    timerUpdate(&s_connectTimer, timeout, true);

    {
        lock_guard<mutex> lk{s_mut};
        TimePoint expiration = Clock::now() + timeout;
        
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

    sockaddr_storage sas;
    copy(&sas, remoteEnd);
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
        logMsgError() << "ConnectEx(" << remoteEnd << "): " << err;
        lock_guard<mutex> lk{s_mut};
        s_connecting.pop_back();
        return pushConnectFailed(notify);
    }
}

//===========================================================================
void ConnSocket::onConnect (
    int error,
    int bytes
) {
    unique_ptr<ConnSocket> hostage(this);

    if (m_mode == ISocketNotify::kClosing) 
        return m_notify->onSocketConnectFailed();

    assert(m_mode == ISocketNotify::kConnecting);

    if (error)
        return m_notify->onSocketConnectFailed();

    //-----------------------------------------------------------------------
    // update socket and start receiving
    if (SOCKET_ERROR == setsockopt(
        m_handle, 
        SOL_SOCKET,
        SO_UPDATE_CONNECT_CONTEXT,
        nullptr,
        0
    )) {
        logMsgError() 
            << "setsockopt(SO_UPDATE_CONNECT_CONTEXT): " 
            << WinError{};
        return m_notify->onSocketConnectFailed();
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
        logMsgError() << "getpeername: " << WinError{};
        return m_notify->onSocketConnectFailed();
    }
    copy(&m_connInfo.remoteEnd, sas);

    // locally bound address
    if (SOCKET_ERROR == getsockname(
        m_handle, 
        (sockaddr *) &sas, 
        &sasLen
    )) {
        logMsgError() << "getsockname: " << WinError{};
        return m_notify->onSocketConnectFailed();
    }
    copy(&m_connInfo.localEnd, sas);

    //-----------------------------------------------------------------------
    // create read/write queue
    if (!createQueue()) 
        return m_notify->onSocketConnectFailed();

    // notify socket connect event
    hostage.release();
    m_notify->onSocketConnect(m_connInfo);
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {
    class ShutdownNotify : public IAppShutdownNotify {
        void onAppStartConsoleCleanup () override;
        bool onAppQueryConsoleDestroy () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onAppStartConsoleCleanup () {
    lock_guard<mutex> lk{s_mut};
    for (auto&& task : s_connecting)
        task.m_socket->hardClose();
}

//===========================================================================
bool ShutdownNotify::onAppQueryConsoleDestroy () {
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
void iSocketConnectInitialize () {
    // Don't register cleanup until all dependents (aka sockbuf) have
    // registered their cleanups (aka been initialized)
    appMonitorShutdown(&s_cleanup);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void socketConnect (
    ISocketNotify * notify,
    const Endpoint & remoteEnd,
    const Endpoint & localEnd,
    Duration timeout
) {
    ConnSocket::connect(notify, remoteEnd, localEnd, timeout);
}

} // namespace
