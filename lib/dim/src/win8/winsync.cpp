// winsync.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;

namespace Dim {


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
void WinEvent::signal () {
    SetEvent(m_handle);
}

//===========================================================================
void WinEvent::wait (Duration wait) {
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
static void __stdcall eventWaitCallback (void * param, uint8_t timeout) {
    auto notify = reinterpret_cast<IWinEventWaitNotify *>(param);
    taskPushEvent(*notify);
}

//===========================================================================
IWinEventWaitNotify::IWinEventWaitNotify () {
    m_overlapped.hEvent = CreateEvent(nullptr, 0, 0, nullptr);
    assert(m_overlapped.hEvent);

    if (!RegisterWaitForSingleObject(
        &m_registeredWait,
        m_overlapped.hEvent,
        &eventWaitCallback,
        this,
        INFINITE,   // timeout
        WT_EXECUTEINWAITTHREAD
    )) {
        logMsgCrash() << "RegisterWaitForSingleObject: " << WinError{};
    }
}

//===========================================================================
IWinEventWaitNotify::~IWinEventWaitNotify () {
    if (m_registeredWait && !UnregisterWaitEx(m_registeredWait, nullptr)) {
        logMsgError() << "UnregisterWaitEx: " << WinError{};
    }
    if (m_overlapped.hEvent && !CloseHandle(m_overlapped.hEvent)) {
        logMsgError() << "CloseHandle(overlapped.hEvent): " << WinError{};
    }
}

} // namespace
