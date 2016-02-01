// winsockacc.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


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

class AcceptSocket : public DimSocket {
public:
    static void Accept (ListenSocket * notify);
public:
    using DimSocket::DimSocket;
    void OnAccept (ListenSocket * listen, int xferError, int xferBytes);
};

class ListenSocket : public IWinEventWaitNotify {
public:
    SOCKET m_handle{INVALID_SOCKET};
    Endpoint m_localEnd;
    unique_ptr<AcceptSocket> m_socket;
    IDimSocketListenNotify * m_notify{nullptr};
    char m_addrBuf[2 * sizeof sockaddr_storage];

public:
    ListenSocket (
        IDimSocketListenNotify * notify,
        const Endpoint & end
    );

    void OnTask () override;
};

class ListenStopTask : public IDimTaskNotify {
    IDimSocketListenNotify * m_notify{nullptr};
public:
    ListenStopTask (IDimSocketListenNotify * notify);
    void OnTask () override;
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
ListenStopTask::ListenStopTask (IDimSocketListenNotify * notify)
    : m_notify(notify)
{}

//===========================================================================
void ListenStopTask::OnTask () {
    m_notify->OnListenStop();
    delete this;
}


/****************************************************************************
*
*   ListenSocket
*
***/

//===========================================================================
ListenSocket::ListenSocket (
    IDimSocketListenNotify * notify,
    const Endpoint & end
) 
    : m_notify{notify}
    , m_localEnd{end}
{}

//===========================================================================
void ListenSocket::OnTask () {
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
    m_socket->OnAccept(this, err, bytesTransferred);
}


/****************************************************************************
*
*   AcceptSocket
*
***/

//===========================================================================
static void PushListenStop (ListenSocket * listen) {
    auto ptr = new ListenStopTask(listen->m_notify);

    {
        lock_guard<mutex> lk{s_mut};
        if (listen->m_handle != INVALID_SOCKET) {
            if (SOCKET_ERROR == closesocket(listen->m_handle))
                DimLog{kCrash} << "closesocket(listen): " << WinError{};
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

    DimTaskPushEvent(*ptr);
}

//===========================================================================
// static
void AcceptSocket::Accept (ListenSocket * listen) {
    assert(!listen->m_socket.get());
    auto sock = make_unique<AcceptSocket>(
        listen->m_notify->OnListenCreateSocket().release()
    );
    sock->m_handle = WinSocketCreate();
    if (sock->m_handle == INVALID_SOCKET) 
        return PushListenStop(listen);

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
        DimLog{kError} << "WSAIoctl(get AcceptEx): " << WinError{};
        return PushListenStop(listen);
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
        DimLog{kError} << "AcceptEx(" << listen->m_localEnd << "): " << err;
        return PushListenStop(listen);
    }
}

//===========================================================================
static bool GetAcceptInfo (
    DimSocketAcceptInfo * out,
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
        DimLog{kError} << "WSAIoctl(get GetAcceptExSockAddrs): " 
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
    DimEndpointFromStorage(&out->localEnd, sas);
    memcpy(&sas, rsa, rsaLen);
    DimEndpointFromStorage(&out->remoteEnd, sas);
    return true;
}
     
//===========================================================================
void AcceptSocket::OnAccept (
    ListenSocket * listen,
    int xferError,
    int xferBytes
) {
    unique_ptr<AcceptSocket> hostage{move(listen->m_socket)};

    DimSocketAcceptInfo info;
    bool ok = !xferError 
        && GetAcceptInfo(&info, m_handle, listen->m_addrBuf);

    Accept(listen);

    if (xferError) {
        DimLog{kError} << "OnAccept: " << WinError(xferError);
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
        DimLog{kError} 
            << "setsockopt(SO_UPDATE_ACCEPT_CONTEXT): " 
            << WinError{};
        return;
    }

    // create read/write queue
    if (!CreateQueue()) 
        return;

    hostage.release();
    m_notify->OnSocketAccept(info);
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {
    class ShutdownNotify : public IDimAppShutdownNotify {
        void OnAppStartConsoleCleanup () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::OnAppStartConsoleCleanup () {
    assert(s_listeners.empty());
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimSocketAcceptInitialize () {
    DimAppMonitorShutdown(&s_cleanup);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
static void PushListenStop (IDimSocketListenNotify * notify) {
    auto ptr = new ListenStopTask(notify);
    DimTaskPushEvent(*ptr);
}

//===========================================================================
void DimSocketListen (
    IDimSocketListenNotify * notify,
    const Endpoint & localEnd
) {
    auto hostage = make_unique<ListenSocket>(notify, localEnd);
    auto sock = hostage.get();
    sock->m_handle = WinSocketCreate(localEnd);
    if (sock->m_handle == INVALID_SOCKET) 
        return PushListenStop(notify); 

    if (SOCKET_ERROR == listen(sock->m_handle, SOMAXCONN)) {
        DimLog{kError} << "listen(SOMAXCONN): " << WinError{};
        if (SOCKET_ERROR == closesocket(sock->m_handle)) 
            DimLog{kError} << "closesocket(listen): " << WinError{};
        return PushListenStop(notify);
    }

    {
        lock_guard<mutex> lk{s_mut};
        s_listeners.push_back(move(hostage));
    }

    AcceptSocket::Accept(sock);
}

//===========================================================================
void DimSocketStop (
    IDimSocketListenNotify * notify,
    const Endpoint & localEnd
) {
    lock_guard<mutex> lk{s_mut};
    for (auto&& ptr : s_listeners) {
        if (ptr->m_notify == notify 
            && ptr->m_localEnd == localEnd
            && ptr->m_handle != INVALID_SOCKET
        ) {
            if (SOCKET_ERROR == closesocket(ptr->m_handle)) {
                DimLog{kError} << "closesocket(listen): " << WinError{};
            }
            ptr->m_handle = INVALID_SOCKET;
            return;
        }
    }
}