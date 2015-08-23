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

class AcceptSocket : public DimSocket {
public:
    static void Accept (
        IDimSocketNotify * notify,
        const SockAddr & localAddr
    );
public:
    using DimSocket::DimSocket;
    void OnAccept (int error, int bytes);
};

class AcceptTask : public IDimTaskNotify {
public:
    unique_ptr<AcceptSocket> m_socket;
public:
    AcceptTask (unique_ptr<AcceptSocket> && sock);
    void OnTask () override;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static mutex s_mut;
static list<AcceptTask> s_accepting;


/****************************************************************************
*
*   AcceptTask
*
***/


/****************************************************************************
*
*   AcceptSocket
*
***/


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
    for (auto&& task : s_accepting)
        task.m_socket->HardClose();
}

//===========================================================================
bool ShutdownNotify::OnAppQueryConsoleDestroy () {
    lock_guard<mutex> lk{s_mut};
    return s_accepting.empty();
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
void DimSocketListen (
    IDimSocketNotify * notify,
    const SockAddr & localAddr
) {
}
