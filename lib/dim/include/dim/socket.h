// socket.h - dim core
#pragma once

#include "dim/config.h"
#include "dim/types.h"

#include <memory>

namespace Dim {

struct SocketConnectInfo {
    Endpoint remoteEnd;
    Endpoint localEnd;
};
struct SocketAcceptInfo {
    Endpoint remoteEnd;
    Endpoint localEnd;
};
struct SocketData {
    char * data;
    int bytes;
};

class ISocketNotify {
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
    virtual ~ISocketNotify () {}

    // for connectors
    virtual void onSocketConnect (const SocketConnectInfo & info) {};
    virtual void onSocketConnectFailed () {};

    // for listeners
    virtual void onSocketAccept (const SocketAcceptInfo & info) {};

    virtual void onSocketRead (const SocketData & data) = 0;
    virtual void onSocketDisconnect () {};

private:
    friend class SocketBase;
    SocketBase * m_socket{nullptr};
};

ISocketNotify::Mode socketGetMode (ISocketNotify * notify);
void socketDisconnect (ISocketNotify * notify);

//===========================================================================
// connect
//===========================================================================
void socketConnect (
    ISocketNotify * notify,
    const Endpoint & remoteEnd,
    const Endpoint & localEnd,
    Duration timeout = {} // 0 for default timeout
);

//===========================================================================
// listen
//===========================================================================
class ISocketListenNotify {
public:
    virtual ~ISocketListenNotify () {}
    virtual void onListenStop () = 0;
    virtual std::unique_ptr<ISocketNotify> onListenCreateSocket () = 0;
};
void socketListen (
    ISocketListenNotify * notify,
    const Endpoint & localEnd
);
void socketStop (
    ISocketListenNotify * notify,
    const Endpoint & localEnd
);

//===========================================================================
// write
//===========================================================================
struct SocketBuffer {
    char * data;
    int len;

    ~SocketBuffer ();
};
std::unique_ptr<SocketBuffer> socketGetBuffer ();

// Writes the data and deletes the buffer.
void socketWrite (
    ISocketNotify * notify, 
    std::unique_ptr<SocketBuffer> buffer,
    size_t bytes
);

} // namespace
