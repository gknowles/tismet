// httpint.h - dim core
#ifndef DIM_HTTPINT_INCLUDED
#define DIM_HTTPINT_INCLUDED

#include <unordered_map>


/****************************************************************************
*
*   Http connection
*
***/

struct DimHttpStream {
    enum State {
        kIdle,
        kLocalReserved,
        kRemoteReserved,
        kOpen,
        kLocalClosed,
        kRemoteClosed,
        kReset,         // sent RST_STREAM, not yet confirmed
        kClosed,
        kDeleted,       // waiting for garbage collection
    };

    State m_state{kIdle};
    TimePoint m_closed;
    std::unique_ptr<DimHttpMsg> m_msg;
};

class DimHttpConn {
public:
    DimHttpConn ();

    // Returns false when no more data will be accepted, either by request
    // of the input or due to error.
    // Even after an error, msgs and reply should be processed.
    //  - msg: zero or more requests, push promises, and/or replies are appended
    //  - reply: data to send to the remote endpoint is appended
    bool Recv (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const void * src, 
        size_t srcLen
    );

    // Serializes a request and returns the stream id used
    int Request (
        CharBuf * out,
        std::unique_ptr<DimHttpMsg> msg
    );

    // Serializes a push promise
    void PushPromise (
        CharBuf * out,
        std::unique_ptr<DimHttpMsg> msg
    );

    // Serializes a reply on the specified stream
    void Reply (
        CharBuf * out,
        int stream,
        std::unique_ptr<DimHttpMsg> msg
    );

    void ResetStream (CharBuf * out, int stream);

    void DeleteStream (int stream, DimHttpStream * sm);

private:
    enum class ByteMode;
    enum class FrameMode;

    DimHttpStream * FindAlways (CharBuf * out, int stream);

    bool OnFrame (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[]
    );
    bool OnContinuation (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnData (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnGoAway (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnHeaders (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnPing (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnPriority (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnPushPromise (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnRstStream (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnSettings (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool OnWindowUpdate (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );

    // byte parsing
    ByteMode m_byteMode;
    int m_inputPos{0};
    std::vector<char> m_input;
    int m_inputFrameLen{0};
    int m_maxInputFrame{16384};

    // frame parsing
    int m_lastInputStream{0};
    FrameMode m_frameMode;
    int m_continueStream{0};

    int m_nextOutputStream{0};
    int m_lastOutputStream{0};
    int m_maxOutputFrame{16384};

    std::unordered_map<int, std::shared_ptr<DimHttpStream>> m_streams;
    DimHpack::Encode m_encoder;
    DimHpack::Decode m_decoder;
};

#endif
