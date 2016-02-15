// httpint.h - dim core
#ifndef DIM_HTTPINT_INCLUDED
#define DIM_HTTPINT_INCLUDED

#include <unordered_map>

namespace Dim {


/****************************************************************************
*
*   Http connection
*
***/

struct HttpStream {
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
    std::unique_ptr<HttpMsg> m_msg;
};

class HttpConn {
public:
    HttpConn ();

    // Returns false when no more data will be accepted, either by request
    // of the input or due to error.
    // Even after an error, msgs and reply should be processed.
    //  - msg: zero or more requests, push promises, and/or replies are appended
    //  - reply: data to send to the remote endpoint is appended
    bool recv (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * reply,
        const void * src, 
        size_t srcLen
    );

    // Serializes a request and returns the stream id used
    int request (
        CharBuf * out,
        std::unique_ptr<HttpMsg> msg
    );

    // Serializes a push promise
    void pushPromise (
        CharBuf * out,
        std::unique_ptr<HttpMsg> msg
    );

    // Serializes a reply on the specified stream
    void reply (
        CharBuf * out,
        int stream,
        std::unique_ptr<HttpMsg> msg
    );

    void resetStream (CharBuf * out, int stream);

    void deleteStream (int stream, HttpStream * sm);

private:
    enum class ByteMode;
    enum class FrameMode;

    HttpStream * findAlways (CharBuf * out, int stream);

    bool onFrame (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[]
    );
    bool onContinuation (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onData (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onGoAway (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onHeaders (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onPing (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onPriority (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onPushPromise (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onRstStream (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onSettings (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
        CharBuf * out,
        const char src[],
        int stream,
        int flags
    );
    bool onWindowUpdate (
        std::list<std::unique_ptr<HttpMsg>> * msgs, 
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

    std::unordered_map<int, std::shared_ptr<HttpStream>> m_streams;
    HpackEncode m_encoder;
    HpackDecode m_decoder;
};

} // namespace

#endif
