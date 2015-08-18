// winsockint.h - dim core - windows platform
#ifndef DIM_WINSOCKINT_INCLUDED
#define DIM_WINSOCKINT_INCLUDED


/****************************************************************************
*
*   DimSocket
*
***/

class DimSocket {
public:
    class RequestTaskBase : public IDimTaskNotify {
        virtual void OnTask () override = 0;
    public:
        RIO_BUF m_rbuf{};
        std::unique_ptr<DimSocketBuffer> m_buffer;

        // filled in after completion
        WinError m_xferError{(WinError::NtStatus) 0};
        int m_xferBytes{0};
        DimSocket * m_socket{nullptr};
    };
    class ReadTask : public RequestTaskBase {
        void OnTask () override;
    };
    class WriteTask : public RequestTaskBase {
        void OnTask () override;
    };

public:
    static RunMode GetMode (IDimSocketNotify * notify);
    static void Disconnect (IDimSocketNotify * notify);
    static void Write (
        IDimSocketNotify * notify, 
        std::unique_ptr<DimSocketBuffer> buffer,
        size_t bytes
    );

public:
    DimSocket (IDimSocketNotify * notify);
    virtual ~DimSocket ();

    void HardClose ();

    bool CreateQueue ();
    void OnRead ();
    void OnWrite (WriteTask * task);

    void QueueRead_LK ();
    void QueueWrite_LK (
        std::unique_ptr<DimSocketBuffer> buffer,
        size_t bytes
    );
    void QueueWriteFromUnsent_LK ();

protected:
    IDimSocketNotify * m_notify{nullptr};
    SOCKET m_handle{INVALID_SOCKET};
    DimSocketConnectInfo m_connInfo;

private:
    RIO_RQ m_rq{};
    
    // has received disconnect and is waiting for writes to complete
    bool m_closing{false};

    // used by single read request
    ReadTask m_read;
    static const int kMaxReceiving{1};

    // used by write requests
    std::list<WriteTask> m_sending;
    int m_numSending{0};
    int m_maxSending{0};
    std::list<WriteTask> m_unsent;
};


/****************************************************************************
*
*   Socket connect
*
***/

class DimConnectSocket : public DimSocket {
public:
    static void Connect (
        IDimSocketNotify * notify,
        const SockAddr & remoteAddr,
        const SockAddr & localAddr
    );
public:
    using DimSocket::DimSocket;
    void OnConnect (int error, int bytes);

};

void IDimSocketConnectInitialize ();


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


#endif
