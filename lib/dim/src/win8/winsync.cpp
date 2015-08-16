// winsync.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;


/****************************************************************************
*
*   WinEvent
*
***/

//===========================================================================
WinEvent::WinEvent () {
    m_handle = CreateEvent(
        NULL,   // security attributes
        false,  // manual reset
        false,  // initial signaled state
        NULL    // name
    );
}

//===========================================================================
WinEvent::~WinEvent () {
    CloseHandle(m_handle);
}

//===========================================================================
void WinEvent::Signal () {
    SetEvent(m_handle);
}

//===========================================================================
void WinEvent::Wait (Duration wait) {
    auto waitMs = duration_cast<milliseconds>(wait);
    if (wait <= 0ms || waitMs >= chrono::milliseconds(INFINITE)) {
        WaitForSingleObject(m_handle, INFINITE);
    } else {
        WaitForSingleObject(m_handle, (DWORD) waitMs.count());
    }
}


/****************************************************************************
*
*   IWinEventWaitNotify
*
***/

//===========================================================================
static void __stdcall EventWaitCallback (void * param, uint8_t timeout) {
    auto notify = reinterpret_cast<IWinEventWaitNotify *>(param);
    DimTaskPushEvent(*notify);
}

//===========================================================================
IWinEventWaitNotify::IWinEventWaitNotify () {
    m_overlapped.hEvent = CreateEvent(nullptr, 0, 0, nullptr);

    if (!RegisterWaitForSingleObject(
        &m_registeredWait,
        m_overlapped.hEvent,
        &EventWaitCallback,
        this,
        INFINITE,   // timeout
        WT_EXECUTEINWAITTHREAD | WT_EXECUTEONLYONCE
    )) {
        DimLog{kCrash} << "RegisterWaitForSingleObject: " << WinError{};
    }
}

//===========================================================================
IWinEventWaitNotify::~IWinEventWaitNotify () {
    if (m_registeredWait && !UnregisterWaitEx(m_registeredWait, nullptr)) {
        DimLog{kError} << "UnregisterWaitEx: " << WinError{};
    }
    if (m_overlapped.hEvent && !CloseHandle(m_overlapped.hEvent)) {
        DimLog{kError} << "CloseHandle(overlapped.hEvent): " << WinError{};
    }
}
