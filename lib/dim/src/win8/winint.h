// winint.h - dim core - windows platform
#ifndef DIM_WININT_INCLUDED
#define DIM_WININT_INCLUDED

#include <iosfwd>


/****************************************************************************
*
*   Overlapped
*
***/

struct WinOverlappedEvent {
    OVERLAPPED overlapped{};
    IDimTaskNotify * notify{nullptr};
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

    void Signal ();
    void Wait (Duration wait = DIM_TIMER_INFINITE);

    HANDLE NativeHandle () const { return m_handle; };

private:
    HANDLE m_handle;
};


/****************************************************************************
*
*   Iocp
*
***/

void WinIocpInitialize ();

bool WinIocpBindHandle (HANDLE handle);


/****************************************************************************
*
*   Wait for events
*
***/

class IWinEventWaitNotify : public IDimTaskNotify {
public:
    IWinEventWaitNotify ();
    ~IWinEventWaitNotify ();

    virtual void OnTask () override = 0;

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

std::ostream & operator<< (std::ostream & os, const WinError & val);


/****************************************************************************
*
*   Socket
*
***/

SOCKET WinSocketCreate ();
SOCKET WinSocketCreate (const Endpoint & localEnd);

#endif
