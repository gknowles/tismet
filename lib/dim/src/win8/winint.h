// winint.h - dim core - windows platform
#pragma once

#include <iosfwd>

namespace Dim {


/****************************************************************************
*
*   Overlapped
*
***/

struct WinOverlappedEvent {
    OVERLAPPED overlapped{};
    ITaskNotify * notify{nullptr};
};


/****************************************************************************
*
*   Event
*
***/

class WinEvent {
public:
    WinEvent ();
    ~WinEvent ();

    void signal ();
    void wait (Duration wait = kTimerInfinite);

    HANDLE nativeHandle () const { return m_handle; };

private:
    HANDLE m_handle;
};


/****************************************************************************
*
*   Iocp
*
***/

void winIocpInitialize ();

bool winIocpBindHandle (HANDLE handle);


/****************************************************************************
*
*   Wait for events
*
***/

class IWinEventWaitNotify : public ITaskNotify {
public:
    IWinEventWaitNotify ();
    ~IWinEventWaitNotify ();

    virtual void onTask () override = 0;

    OVERLAPPED m_overlapped{};
    HANDLE m_registeredWait{nullptr};
};


/****************************************************************************
*
*   Error
*
***/

class WinError {
public:
    enum NtStatus;

public:
    // default constructor calls GetLastError()
    WinError ();
    WinError (NtStatus status);
    WinError (int error);

    WinError & operator= (int error);
    // sets equivalent standard windows error value
    WinError & operator= (NtStatus status);

    operator int () const { return m_value; }

private:
    int m_value;
};

::std::ostream & operator<< (::std::ostream & os, const WinError & val);


/****************************************************************************
*
*   Socket
*
***/

SOCKET winSocketCreate ();
SOCKET winSocketCreate (const Endpoint & localEnd);

} // namespace
