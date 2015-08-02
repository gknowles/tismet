// socket.h - dim core
#ifndef DIM_SOCKET_INCLUDED
#define DIM_SOCKET_INCLUDED

#include "dim/config.h"

#include "dim/types.h"

class IDimSocketListenNotify {
};

struct DimSocketConnectInfo {
    SockAddr remoteAddr;
    SockAddr localAddr;
};
struct DimSocketData {
    char * data;
    size_t bytes;
};

class IDimSocketNotify {
public:
    virtual ~IDimSocketNotify ();

    virtual void OnSocketConnect (const DimSocketConnectInfo & info) {};
    virtual void OnSocketConnectFailed () {};
    virtual void OnSocketRead (const DimSocketData & data) = 0;
    virtual void OnSocketDisconnect () {};

private:
    friend class DimSocket;
    DimSocket * m_socket = nullptr;
};

void DimSocketConnect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
);
void DimSocketDisconnect (IDimSocketNotify * notify);
void DimSocketWrite (IDimSocketNotify * notify, void * data, size_t bytes);

#endif
