// http.h - dim services
//
// implements http/2, as defined by:
//  rfc7540 - Hypertext Transfer Protocol Version 2 (HTTP/2)
//  rfc7541 - HPACK: Header Compression for HTTP/2

#ifndef DIM_HTTP_INCLUDED
#define DIM_HTTP_INCLUDED

#include "dim/config.h"

#include <cassert>
#include <map>
#include <set>

enum HttpHdr {
    kHttpInvalid,
    kHttpCacheControl,
    kHttpConnection,
    kHttpContentEncoding,
    kHttpContentLength,
    kHttpContentType,
    kHttpCookie,
    kHttpDate,
    kHttpForwardedFor,
    kHttpHost,
    kHttpIfMatch,
    kHttpIfModified_since,
    kHttpIfNoneMatch,
    kHttpLastModified,
    kHttpServer,
    kHttpSetCookie,
};

class DimHttpMsg {
public:
    struct Hdr;
    struct HdrValue;
    class HdrRange;
    class HdrIterator;

public:
    const char * Method () const;
    const char * Scheme () const;
    const char * Authority () const;
    const char * Path () const;
    const char * Fragment () const;
    
    int Status () const;

    const Hdr * FindFirst (int header) const;
    Hdr * FindFirst (int header);
    const Hdr * FindLast (int header) const;
    Hdr * FindLast (int header);
    const Hdr * Next (const Hdr * hdr) const;
    Hdr * Next (Hdr * hdr);
    const Hdr * Prev (const Hdr * hdr) const;
    Hdr * Prev (Hdr * hdr);

    HdrRange Headers (
        int header = kHttpInvalid // defaults to all
    ) const;

    CharBuf * Data ();
    const CharBuf * Data () const;

private:
    CharBuf m_data;
};

struct DimHttpMsg::Hdr {
    HttpHdr Id () const;
    const char * Name () const;
    const char * Value () const;
private:
    ~Hdr ();

    HttpHdr m_id{kHttpInvalid};
    const char * m_name{nullptr};
    DimHttpMsg::HdrValue * value{nullptr};
    Hdr * m_next{nullptr};
};

struct DimHttpMsg::HdrValue {
    const char * value;
private:
    HdrValue * m_next{nullptr};
    ~HdrValue ();
};

class DimHttpMsg::HdrIterator {
    DimHttpMsg * m_msg{nullptr};
    Hdr * m_current{nullptr};
public:
    HdrIterator ();
    HdrIterator (DimHttpMsg & msg, int header);
    auto operator++ () {
        if (m_msg)
            m_current = m_msg->Next(m_current);
        return *this;
    }
    Hdr & operator* () {
        assert(m_current);
        return *m_current;
    }
};

class DimHttpMsg::HdrRange {
    DimHttpMsg::HdrRange (DimHttpMsg & msg, int header);
    DimHttpMsg::HdrIterator it;

    DimHttpMsg::HdrIterator begin () { return it; }
    DimHttpMsg::HdrIterator end () { return DimHttpMsg::HdrIterator(); }
};

struct DimHttpStream {
    // if msg is empty, the msg has been passed on to the application but
    // the app hasn't yet sent a reply
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
        int srcLen
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

private:
    enum class ByteMode;
    enum class FrameMode;
    bool OnFrame (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[]
    );
    bool OnContinuation (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnData (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnGoAway (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnHeaders (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnPing (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnPriority (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnPushPromise (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnRstStream (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnSettings (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        const char src[],
        int stream,
        int flags
    );
    bool OnWindowUpdate (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
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

    std::map<int, DimHttpStream> m_streams;
    std::set<unsigned> m_closedStreams;
};

#endif
