// winint.h - dim core - windows platform
#ifndef DIM_WININT_INCLUDED
#define DIM_WININT_INCLUDED

#include <iosfwd>


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

struct WinIocpEvent {
    OVERLAPPED overlapped;
    IDimTaskNotify * notify;
};

void WinIocpInitialize ();

bool WinIocpBindHandle (HANDLE handle);


/****************************************************************************
*
*   Socket buffers
*
***/

void IDimSocketBufferInitialize (RIO_EXTENSION_FUNCTION_TABLE & rio);
void IDimSocketGetRioBuffer (
    RIO_BUF * out, 
    DimSocketBuffer * buf,
    size_t bytes
);


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

    OVERLAPPED m_overlapped = {};
    HANDLE m_registeredWait = NULL;
};


/****************************************************************************
*
*   Error
*
***/

class WinError {
    int m_value;
public:
    // default constructor calls GetLastError()
    WinError ();

    operator int () const { return m_value; }
};
std::ostream & operator<< (std::ostream & os, const WinError & val);


#endif
