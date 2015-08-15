// winiocp.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


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
static condition_variable s_shutdownCv; // when dispatch thread ends
static mutex s_mut;


/****************************************************************************
*
*   Iocp thread
*
***/

//===========================================================================
static void IocpDispatchThread () {
    OVERLAPPED * overlapped;
    ULONG_PTR key;
    ULONG bytes;
    for (;;) {
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
                DimLog{kCrash} << "GetQueuedCompletionStatus: "
                    << err;
            }
        }

        auto evt = (WinIocpEvent *) overlapped;
        DimTaskPushEvent(*evt->notify);
    }

    s_iocp = 0;
    s_shutdownCv.notify_one();
}


/****************************************************************************
*
*   WinIocpShutdown
*
***/

namespace {
class WinIocpShutdown : public IDimAppShutdownNotify {
    bool OnAppQueryConsoleDestroy () override;
};
static WinIocpShutdown s_cleanup;
} // namespace

//===========================================================================
bool WinIocpShutdown::OnAppQueryConsoleDestroy () {
    s_mode = kRunStopping;
    if (!CloseHandle(s_iocp))
        DimLog{kError} << "CloseHandle(iocp): " << WinError{};

    unique_lock<mutex> lk(s_mut);
    while (s_iocp)
        s_shutdownCv.wait(lk);

    s_mode = kRunStopped;
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void WinIocpInitialize () {
    s_mode = kRunStarting;
    DimAppMonitorShutdown(&s_cleanup);

    s_iocp = CreateIoCompletionPort(
        INVALID_HANDLE_VALUE,
        NULL,   // existing port
        NULL,   // completion key
        0       // num threads, 0 for default
    );
    if (!s_iocp) {
        DimLog{kCrash} << "CreateIoCompletionPort(null): " << WinError{};
    }

    thread thr{IocpDispatchThread};
    thr.detach();

    s_mode = kRunRunning;
}

//===========================================================================
bool WinIocpBindHandle (HANDLE handle) {
    assert(s_iocp);

    if (!CreateIoCompletionPort(
        handle,
        s_iocp,
        NULL,
        0
    )) {
        DimLog{kError} << "CreateIoCompletionPort(handle): " << WinError{};
        return false;
    }

    return true;
}
