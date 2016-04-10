// winsockint.h - dim core - windows platform
#pragma once

#include <list>
#include <memory>

namespace Dim {


/****************************************************************************
*
*   DimSocket
*
***/

class SocketRequestTaskBase : public ITaskNotify {
    virtual void onTask () override = 0;
public:
    RIO_BUF m_rbuf{};
    std::unique_ptr<SocketBuffer> m_buffer;

    // filled in after completion
    WinError m_xferError{(WinError::NtStatus) 0};
    int m_xferBytes{0};
    SocketBase * m_socket{nullptr};
};

class SocketReadTask : public SocketRequestTaskBase {
    void onTask () override;
};

class SocketWriteTask : public SocketRequestTaskBase {
    void onTask () override;
};

class SocketBase {
public:
    using Mode = ISocketNotify::Mode;

public:
    static SocketBase::Mode getMode (ISocketNotify * notify);
    static void disconnect (ISocketNotify * notify);
    static void write (
        ISocketNotify * notify, 
        std::unique_ptr<SocketBuffer> buffer,
        size_t bytes
    );

public:
    SocketBase (ISocketNotify * notify);
    virtual ~SocketBase ();

    void hardClose ();

    bool createQueue ();
    void onRead ();
    void onWrite (SocketWriteTask * task);

    void queueRead_LK ();
    void queueWrite_LK (
        std::unique_ptr<SocketBuffer> buffer,
        size_t bytes
    );
    void queueWriteFromUnsent_LK ();

protected:
    ISocketNotify * m_notify{nullptr};
    SOCKET m_handle{INVALID_SOCKET};
    SocketConnectInfo m_connInfo;
    Mode m_mode{Mode::kInactive};

private:
    RIO_RQ m_rq{};
    
    // used by single read request
    SocketReadTask m_read;
    static const int kMaxReceiving{1};

    // used by write requests
    std::list<SocketWriteTask> m_sending;
    int m_numSending{0};
    int m_maxSending{0};
    std::list<SocketWriteTask> m_unsent;
};


/****************************************************************************
*
*   Socket accept
*
***/

void iSocketAcceptInitialize ();


/****************************************************************************
*
*   Socket connect
*
***/

void iSocketConnectInitialize ();


/****************************************************************************
*
*   Socket buffers
*
***/

void iSocketBufferInitialize (RIO_EXTENSION_FUNCTION_TABLE & rio);
void iSocketGetRioBuffer (
    RIO_BUF * out, 
    SocketBuffer * buf,
    size_t bytes
);

} // namespace
