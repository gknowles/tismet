// winiocp.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Incomplete public types
*
***/

/****************************************************************************
*
*   Private declarations
*
***/

namespace {

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static RunMode s_mode{kRunStopped};
static HANDLE s_iocp;
static mutex s_mut;


/****************************************************************************
*
*   Iocp thread
*
***/

//===========================================================================
static void iocpDispatchThread () {
    OVERLAPPED * overlapped;
    ULONG_PTR key;
    ULONG bytes;
    for (;;) {
        // TODO: use GetQueuedCompletionStatusEx and array version of
        // DimTaskPushEvent
        if (!GetQueuedCompletionStatus(
            s_iocp,
            &bytes,
            &key,
            &overlapped,
            INFINITE
        )) {
            WinError err;
            if (err == ERROR_ABANDONED_WAIT_0) {
                // completion port was closed
                break;
            } else if (err == ERROR_OPERATION_ABORTED) {
                // probably file handle was closed
            } else {
                logMsgCrash() << "GetQueuedCompletionStatus: "
                    << err;
            }
        }

        auto evt = (WinOverlappedEvent *) overlapped;
        taskPushEvent(*evt->notify);
    }

    lock_guard<mutex> lk{s_mut};
    s_iocp = 0;
}


/****************************************************************************
*
*   WinIocpShutdown
*
***/

namespace {
class WinIocpShutdown : public IAppShutdownNotify {
    bool onAppQueryConsoleDestroy () override;
};
static WinIocpShutdown s_cleanup;
} // namespace

//===========================================================================
bool WinIocpShutdown::onAppQueryConsoleDestroy () {
    if (s_mode != kRunStopping) {
        s_mode = kRunStopping;
        if (!CloseHandle(s_iocp))
            logMsgError() << "CloseHandle(iocp): " << WinError{};

        Sleep(0);
    }

    bool closed;
    {
        unique_lock<mutex> lk(s_mut);
        closed = !s_iocp;
    }
    if (!closed)
        return appQueryDestroyFailed();

    s_mode = kRunStopped;
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void winIocpInitialize () {
    s_mode = kRunStarting;
    appMonitorShutdown(&s_cleanup);

    s_iocp = CreateIoCompletionPort(
        INVALID_HANDLE_VALUE,
        NULL,   // existing port
        NULL,   // completion key
        0       // num threads, 0 for default
    );
    if (!s_iocp) 
        logMsgCrash() << "CreateIoCompletionPort(null): " << WinError{};

    thread thr{iocpDispatchThread};
    thr.detach();

    s_mode = kRunRunning;
}

//===========================================================================
bool winIocpBindHandle (HANDLE handle) {
    assert(s_iocp);

    if (!CreateIoCompletionPort(
        handle,
        s_iocp,
        NULL,
        0
    )) {
        logMsgError() << "CreateIoCompletionPort(handle): " << WinError{};
        return false;
    }

    return true;
}

} // namespace
