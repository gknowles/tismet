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
    int bytes;
};

class IDimSocketNotify {
public:
    enum Mode {
        kInactive,      // not connected
        kConnecting,
        kActive,        // actively reading
        kClosing,       // closed the handle
        kClosed,        // final zero-length read received
    };

public:
    virtual ~IDimSocketNotify () {}

    virtual void OnSocketConnect (const DimSocketConnectInfo & info) {};
    virtual void OnSocketConnectFailed () {};
    virtual void OnSocketRead (const DimSocketData & data) = 0;
    virtual void OnSocketDisconnect () {};

private:
    friend class DimSocket;
    DimSocket * m_socket{nullptr};
};


IDimSocketNotify::Mode DimSocketGetMode (IDimSocketNotify * notify);

//===========================================================================
// connecting and disconnecting
//===========================================================================
void DimSocketConnect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr,
    Duration timeout = {} // 0 for default timeout
);
void DimSocketDisconnect (IDimSocketNotify * notify);

//===========================================================================
// writing
//===========================================================================
struct DimSocketBuffer {
    char * data;
    int len;

    ~DimSocketBuffer ();
};
std::unique_ptr<DimSocketBuffer> DimSocketGetBuffer ();

// Writes the data and deletes the buffer.
void DimSocketWrite (
    IDimSocketNotify * notify, 
    std::unique_ptr<DimSocketBuffer> buffer,
    size_t bytes
);

#endif
