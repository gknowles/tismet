// winint.h - dim core - windows platform
#ifndef DIM_WININT_INCLUDED
#define DIM_WININT_INCLUDED


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

struct DimIocpEvent {
    OVERLAPPED overlapped;
    IDimTaskNotify * notify;
};

void DimIocpInitialize ();

HANDLE DimIocpHandle ();


/****************************************************************************
*
*   Socket buffers
*
***/

void IDimSocketBufferInitialize (RIO_EXTENSION_FUNCTION_TABLE & rio);
void IDimSocketGetRioBuffer (RIO_BUF * out, DimSocketBuffer * buf);


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


#endif
