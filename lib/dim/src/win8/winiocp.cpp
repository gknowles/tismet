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
    DWORD error;
    for (;;) {
        if (!GetQueuedCompletionStatus(
            s_iocp,
            &bytes,
            &key,
            &overlapped,
            INFINITE
        )) {
            error = GetLastError();
            if (error == ERROR_ABANDONED_WAIT_0) {
                // completion port was closed
                break;
            }

            DimErrorLog{kFatal} << "GetQueuedCompletionStatusEx, "
                << error;
        }

        auto evt = (DimIocpEvent *) overlapped;
        DimTaskPushEvent(*evt->notify);
    }

    s_iocp = 0;
    s_shutdownCv.notify_one();
}


/****************************************************************************
*
*   DimIocpShutdown
*
***/

namespace {
class DimIocpShutdown : public IDimAppShutdownNotify {
    bool OnAppQueryConsoleDestroy () override;
};
static DimIocpShutdown s_cleanup;
} // namespace

//===========================================================================
bool DimIocpShutdown::OnAppQueryConsoleDestroy () {
    s_mode = kRunStopping;
    if (!CloseHandle(s_iocp))
        DimErrorLog{kError} << "CloseHandle(iocp), " << GetLastError();

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
void DimIocpInitialize () {
    s_mode = kRunStarting;
    DimAppMonitorShutdown(&s_cleanup);

    s_iocp = CreateIoCompletionPort(
        INVALID_HANDLE_VALUE,
        NULL,   // existing port
        0,      // completion key
        0       // num threads, 0 for default
    );
    if (!s_iocp) {
        DimErrorLog{kFatal} << "CreateIoCompletionPort failed, " 
            << GetLastError();
    }

    thread thr{IocpDispatchThread};
    thr.detach();

    s_mode = kRunRunning;
}

//===========================================================================
HANDLE DimIocpHandle () {
    return s_iocp;
}
