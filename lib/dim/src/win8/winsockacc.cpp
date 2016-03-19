// winsockacc.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/



/****************************************************************************
*
*   Private declarations
*
***/

namespace {

class ListenSocket;


class AcceptSocket : public SocketBase {
public:
    static void accept (ListenSocket * notify);
public:
    using SocketBase::SocketBase;
    void onAccept (ListenSocket * listen, int xferError, int xferBytes);
};

class ListenSocket : public IWinEventWaitNotify {
public:
    SOCKET m_handle{INVALID_SOCKET};
    Endpoint m_localEnd;
    unique_ptr<AcceptSocket> m_socket;
    ISocketListenNotify * m_notify{nullptr};
    char m_addrBuf[2 * sizeof sockaddr_storage];

public:
    ListenSocket (
        ISocketListenNotify * notify,
        const Endpoint & end
    );

    void onTask () override;
};

class ListenStopTask : public ITaskNotify {
    ISocketListenNotify * m_notify{nullptr};
public:
    ListenStopTask (ISocketListenNotify * notify);
    void onTask () override;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static mutex s_mut;
static list<unique_ptr<ListenSocket>> s_listeners;


/****************************************************************************
*
*   ListenStopTask
*
***/

//===========================================================================
ListenStopTask::ListenStopTask (ISocketListenNotify * notify)
    : m_notify(notify)
{}

//===========================================================================
void ListenStopTask::onTask () {
    m_notify->onListenStop();
    delete this;
}


/****************************************************************************
*
*   ListenSocket
*
***/

//===========================================================================
ListenSocket::ListenSocket (
    ISocketListenNotify * notify,
    const Endpoint & end
) 
    : m_notify{notify}
    , m_localEnd{end}
{}

//===========================================================================
void ListenSocket::onTask () {
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
    m_socket->onAccept(this, err, bytesTransferred);
}


/****************************************************************************
*
*   AcceptSocket
*
***/

//===========================================================================
static void pushListenStop (ListenSocket * listen) {
    auto ptr = new ListenStopTask(listen->m_notify);

    {
        lock_guard<mutex> lk{s_mut};
        if (listen->m_handle != INVALID_SOCKET) {
            if (SOCKET_ERROR == closesocket(listen->m_handle))
                logMsgCrash() << "closesocket(listen): " << WinError{};
            listen->m_handle = INVALID_SOCKET;
        }
        auto it = s_listeners.begin();
        for (; it != s_listeners.end(); ++it) {
            if (it->get() == listen) {
                s_listeners.erase(it);
                break;
            }
        }
    }

    taskPushEvent(*ptr);
}

//===========================================================================
// static
void AcceptSocket::accept (ListenSocket * listen) {
    assert(!listen->m_socket.get());
    auto sock = make_unique<AcceptSocket>(
        listen->m_notify->onListenCreateSocket().release()
    );
    sock->m_handle = winSocketCreate();
    if (sock->m_handle == INVALID_SOCKET) 
        return pushListenStop(listen);

    // get AcceptEx function
    GUID extId = WSAID_ACCEPTEX;
    LPFN_ACCEPTEX fAcceptEx;
    DWORD bytes;
    if (WSAIoctl(
        sock->m_handle,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &fAcceptEx, sizeof(fAcceptEx),
        &bytes,
        nullptr,    // overlapped
        nullptr     // completion routine
    )) {
        logMsgError() << "WSAIoctl(get AcceptEx): " << WinError{};
        return pushListenStop(listen);
    }

    sock->m_mode = Mode::kAccepting;
    listen->m_socket = move(sock);

    bool error = !fAcceptEx(
        listen->m_handle,
        listen->m_socket->m_handle,
        listen->m_addrBuf,
        0,  // receive data length
        sizeof sockaddr_storage, // localEnd length
        sizeof sockaddr_storage, // remoteEnd length
        nullptr, // bytes received
        &listen->m_overlapped
    );
    WinError err;
    if (!error || err != ERROR_IO_PENDING) {
        logMsgError() << "AcceptEx(" << listen->m_localEnd << "): " << err;
        return pushListenStop(listen);
    }
}

//===========================================================================
static bool getAcceptInfo (
    SocketAcceptInfo * out,
    SOCKET s,
    void * buffer
) {
    GUID extId = WSAID_GETACCEPTEXSOCKADDRS;
    LPFN_GETACCEPTEXSOCKADDRS fGetAcceptExSockAddrs;
    DWORD bytes;
    if (WSAIoctl(
        s,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &fGetAcceptExSockAddrs, sizeof(fGetAcceptExSockAddrs),
        &bytes,
        nullptr,    // overlapped
        nullptr     // completion routine
    )) {
        logMsgError() << "WSAIoctl(get GetAcceptExSockAddrs): " 
            << WinError{};
        return false;
    }

    sockaddr * lsa;
    int lsaLen;
    sockaddr * rsa;    
    int rsaLen;
    fGetAcceptExSockAddrs(
        buffer, 
        0, 
        sizeof(sockaddr_storage),
        sizeof(sockaddr_storage),
        &lsa,
        &lsaLen,
        &rsa,
        &rsaLen
    );
    
    sockaddr_storage sas;
    memcpy(&sas, lsa, lsaLen);
    copy(&out->localEnd, sas);
    memcpy(&sas, rsa, rsaLen);
    copy(&out->remoteEnd, sas);
    return true;
}
     
//===========================================================================
void AcceptSocket::onAccept (
    ListenSocket * listen,
    int xferError,
    int xferBytes
) {
    unique_ptr<AcceptSocket> hostage{move(listen->m_socket)};

    SocketAcceptInfo info;
    bool ok = !xferError 
        && getAcceptInfo(&info, m_handle, listen->m_addrBuf);

    accept(listen);

    if (xferError) {
        logMsgError() << "onAccept: " << WinError(xferError);
        return;
    }
    if (!ok)
        return;

    if (SOCKET_ERROR == setsockopt(
        m_handle, 
        SOL_SOCKET,
        SO_UPDATE_ACCEPT_CONTEXT,
        (char *) &listen->m_handle,
        sizeof listen->m_handle
    )) {
        logMsgError() 
            << "setsockopt(SO_UPDATE_ACCEPT_CONTEXT): " 
            << WinError{};
        return;
    }

    // create read/write queue
    if (!createQueue()) 
        return;

    hostage.release();
    m_notify->onSocketAccept(info);
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {
    class ShutdownNotify : public IAppShutdownNotify {
        void onAppStartConsoleCleanup () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onAppStartConsoleCleanup () {
    assert(s_listeners.empty());
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iSocketAcceptInitialize () {
    appMonitorShutdown(&s_cleanup);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
static void pushListenStop (ISocketListenNotify * notify) {
    auto ptr = new ListenStopTask(notify);
    taskPushEvent(*ptr);
}

//===========================================================================
void socketListen (
    ISocketListenNotify * notify,
    const Endpoint & localEnd
) {
    auto hostage = make_unique<ListenSocket>(notify, localEnd);
    auto sock = hostage.get();
    sock->m_handle = winSocketCreate(localEnd);
    if (sock->m_handle == INVALID_SOCKET) 
        return pushListenStop(notify); 

    if (SOCKET_ERROR == listen(sock->m_handle, SOMAXCONN)) {
        logMsgError() << "listen(SOMAXCONN): " << WinError{};
        if (SOCKET_ERROR == closesocket(sock->m_handle)) 
            logMsgError() << "closesocket(listen): " << WinError{};
        return pushListenStop(notify);
    }

    {
        lock_guard<mutex> lk{s_mut};
        s_listeners.push_back(move(hostage));
    }

    AcceptSocket::accept(sock);
}

//===========================================================================
void socketStop (
    ISocketListenNotify * notify,
    const Endpoint & localEnd
) {
    lock_guard<mutex> lk{s_mut};
    for (auto&& ptr : s_listeners) {
        if (ptr->m_notify == notify 
            && ptr->m_localEnd == localEnd
            && ptr->m_handle != INVALID_SOCKET
        ) {
            if (SOCKET_ERROR == closesocket(ptr->m_handle)) {
                logMsgError() << "closesocket(listen): " << WinError{};
            }
            ptr->m_handle = INVALID_SOCKET;
            return;
        }
    }
}

} // namespace
