// socket.h - dim core
#ifndef DIM_SOCKET_INCLUDED
#define DIM_SOCKET_INCLUDED

#include "dim/config.h"

#include "dim/types.h"

struct DimSocketConnectInfo {
    Endpoint remoteEnd;
    Endpoint localEnd;
};
struct DimSocketAcceptInfo {
    Endpoint remoteEnd;
    Endpoint localEnd;
};
struct DimSocketData {
    char * data;
    int bytes;
};

class IDimSocketNotify {
public:
    enum Mode {
        kInactive,      // not connected
        kAccepting,
        kConnecting,
        kActive,        // actively reading
        kClosing,       // closed the handle
        kClosed,        // final zero-length read received
    };

public:
    virtual ~IDimSocketNotify () {}

    // for connectors
    virtual void OnSocketConnect (const DimSocketConnectInfo & info) {};
    virtual void OnSocketConnectFailed () {};

    // for listeners
    virtual void OnSocketAccept (const DimSocketAcceptInfo & info) {};

    virtual void OnSocketRead (const DimSocketData & data) = 0;
    virtual void OnSocketDisconnect () {};

private:
    friend class DimSocket;
    DimSocket * m_socket{nullptr};
};

IDimSocketNotify::Mode DimSocketGetMode (IDimSocketNotify * notify);
void DimSocketDisconnect (IDimSocketNotify * notify);

//===========================================================================
// connect
//===========================================================================
void DimSocketConnect (
    IDimSocketNotify * notify,
    const Endpoint & remoteEnd,
    const Endpoint & localEnd,
    Duration timeout = {} // 0 for default timeout
);

//===========================================================================
// listen
//===========================================================================
class IDimSocketListenNotify {
public:
    virtual ~IDimSocketListenNotify () {}
    virtual void OnListenStop () = 0;
    virtual std::unique_ptr<IDimSocketNotify> OnListenCreateSocket () = 0;
};
void DimSocketListen (
    IDimSocketListenNotify * notify,
    const Endpoint & localEnd
);
void DimSocketStop (
    IDimSocketListenNotify * notify,
    const Endpoint & localEnd
);

//===========================================================================
// write
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
