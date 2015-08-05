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
    m_handle = CreateEvent(NULL, false, false, NULL);
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
