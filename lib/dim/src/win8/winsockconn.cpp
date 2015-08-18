// winsockconn.cpp - dim core - windows platform
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

class ConnectTask : public IWinEventWaitNotify {
public:
    unique_ptr<DimConnectSocket> m_socket;
public:
    ConnectTask (unique_ptr<DimConnectSocket> && sock);
    void OnTask () override;
};

class ConnectFailedTask : public IDimTaskNotify {
    IDimSocketNotify * m_notify;
public:
    ConnectFailedTask (IDimSocketNotify * notify);
    void OnTask () override;
};

} // namespace


/****************************************************************************
*
*   Incomplete public types
*
***/

/****************************************************************************
*
*   Variables
*
***/


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   ConnectTask
*
***/

//===========================================================================
ConnectTask::ConnectTask (unique_ptr<DimConnectSocket> && sock) 
    : m_socket(move(sock))
{}

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
    m_socket->OnConnect(err, bytesTransferred);
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
*   DimConnectSocket
*
***/

//===========================================================================
static void PushConnectFailed (IDimSocketNotify * notify) {
    auto ptr = new ConnectFailedTask(notify);
    DimTaskPushEvent(*ptr);
}

//===========================================================================
// static
void DimConnectSocket::Connect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    assert(GetMode(notify) == kRunStopped);
    auto sock = make_unique<DimConnectSocket>(notify);
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
        WinError err;
        if (err != ERROR_IO_PENDING) {
            DimLog{kError} << "ConnectEx(" << remoteAddr << "): " << err;
            return PushConnectFailed(notify);
        }
    }
    task.release();
}

//===========================================================================
void DimConnectSocket::OnConnect (
    int error,
    int bytes
) {
    unique_ptr<DimConnectSocket> hostage(this);

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
        bool OnAppQueryConsoleDestroy () override;
    };
} // namespace
static ShutdownNotify s_cleanup;

//===========================================================================
bool ShutdownNotify::OnAppQueryConsoleDestroy () {
    return true;
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
    const SockAddr & localAddr
) {
    DimConnectSocket::Connect(notify, remoteAddr, localAddr);
}

