// winint.h - dim core - windows platform
#ifndef DIM_WININT_INCLUDED
#define DIM_WININT_INCLUDED


/****************************************************************************
*
*   IoOp
*
***/

class IDimIocpNotify;

struct DimIocpEvent {
    OVERLAPPED overlapped;
    IDimIocpNotify * notify;
};

class IDimIocpNotify {
public:
    virtual void OnIocpEvent (DimIocpEvent & evt) = 0;

private:
    HANDLE m_iocpPort;
};


/****************************************************************************
*
*   IoOp
*
***/

void IDimIocpInitialize ();

#endif
