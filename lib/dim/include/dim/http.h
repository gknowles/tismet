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

namespace Dim {

/****************************************************************************
*
*   Constants
*
***/

enum HttpHdr {
    kHttpInvalid,
    KHttp_Authority,
    kHttp_Method,
    kHttp_Path,
    kHttp_Schema,
    kHttp_Status,
    kHttpAccept,
    kHttpAcceptCharset,
    kHttpAcceptEncoding,
    kHttpAcceptLanguage,
    kHttpAcceptRanges,
    kHttpAccessControlAllowOrigin,
    kHttpAge,
    kHttpAllow,
    kHttpAuthorization,
    kHttpCacheControl,
    kHttpConnection,
    kHttpContentDisposition,
    kHttpContentEncoding,
    kHttpContentLanguage,
    kHttpContentLength,
    kHttpContentLocation,
    kHttpContentRange,
    kHttpContentType,
    kHttpCookie,
    kHttpDate,
    kHttpETag,
    kHttpExpect,
    kHttpExpires,
    kHttpForwardedFor,
    kHttpFrom,
    kHttpHost,
    kHttpIfMatch,
    kHttpIfModifiedSince,
    kHttpIfNoneMatch,
    kHttpIfRange,
    kHttpIfUnmodifiedSince,
    kHttpLastModified,
    kHttpLink,
    kHttpLocation,
    kHttpMaxForwards,
    kHttpProxyAuthenticate,
    kHttpProxyAuthorization,
    kHttpRange,
    kHttpReferer,
    kHttpRefresh,
    kHttpRetryAfter,
    kHttpServer,
    kHttpSetCookie,
    kHttpStrictTransportSecurity,
    kHttpTransferEncoding,
    kHttpUserAgent,
    kHttpVary,
    kHttpVia,
    kHttpWwwAuthenticate,
};


/****************************************************************************
*
*   Http message
*
***/

class HttpMsg {
public:
    struct Hdr;
    struct HdrValue;
    class HdrRange;
    class HdrIterator;

public:
    virtual ~HttpMsg () {}

    const Hdr * findFirst (int header) const;
    Hdr * findFirst (int header);
    const Hdr * findLast (int header) const;
    Hdr * findLast (int header);
    const Hdr * next (const Hdr * hdr) const;
    Hdr * next (Hdr * hdr);
    const Hdr * prev (const Hdr * hdr) const;
    Hdr * prev (Hdr * hdr);

    void addHeader (HttpHdr id, const char value[]);
    void addHeader (const char name[], const char value[]);

    // When adding references the memory referenced by the name and value 
    // pointers must be valid for the life of the http msg, such as constants 
    // or strings allocated from this messages Heap().
    void addHeaderRef (HttpHdr id, const char value[]);
    void addHeaderRef (const char name[], const char value[]);

    HdrRange headers (
        int header = kHttpInvalid // defaults to all
    ) const;

    CharBuf * body ();
    const CharBuf * body () const;

    ITempHeap & heap ();

protected:
    virtual bool checkPseudoHeaders () const = 0;

    enum {
        kFlagHasStatus = 0x01,
        kFlagHasMethod = 0x02,
        kFlagHasScheme = 0x04,
        kFlagHasAuthority = 0x08,
        kFlagHasPath = 0x10,
        kFlagHasHeader = 0x20,
    };
    int m_flags{0}; // kFlag*

private:
    CharBuf m_data;
    TempHeap m_heap;
};

struct HttpMsg::Hdr {
    HttpHdr id () const;
    const char * name () const;
    const char * value () const;
private:
    ~Hdr ();

    HttpHdr m_id{kHttpInvalid};
    const char * m_name{nullptr};
    HttpMsg::HdrValue * m_value{nullptr};
    Hdr * m_next{nullptr};
};

class HttpMsg::HdrIterator {
    HttpMsg * m_msg{nullptr};
    Hdr * m_current{nullptr};
public:
    HdrIterator ();
    HdrIterator (HttpMsg & msg, int header);
    auto operator++ () {
        if (m_msg)
            m_current = m_msg->next(m_current);
        return *this;
    }
    Hdr & operator* () {
        assert(m_current);
        return *m_current;
    }
};

class HttpMsg::HdrRange {
    HttpMsg::HdrRange (HttpMsg & msg, int header);
    HttpMsg::HdrIterator it;

    HttpMsg::HdrIterator begin () { return it; }
    HttpMsg::HdrIterator end () { return HttpMsg::HdrIterator(); }
};


class HttpRequest : public HttpMsg {
public:
    const char * method () const;
    const char * scheme () const;
    const char * authority () const;

    // includes path, query, and fragment
    const char * pathAbsolute () const;

    const char * path () const;
    const char * query () const;
    const char * fragment () const;
    
private:
    bool checkPseudoHeaders () const override;
};

class HttpResponse : public HttpMsg {
public:
    int status () const;

private:
    bool checkPseudoHeaders () const override;
};


/****************************************************************************
*
*   Http connection context
*
***/

struct HttpConnHandle : HandleBase {};

HttpConnHandle httpConnect (CharBuf * out);
HttpConnHandle httpListen ();

void httpClose (HttpConnHandle conn);

// Returns false when no more data will be accepted, either by request
// of the input or due to error.
// Even after an error, msgs and out should be processed.
//  - msg: zero or more requests, push promises, and/or replies are appended
//  - out: data to send to the remote endpoint is appended
bool httpRecv (
    HttpConnHandle conn,
    std::list<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
    const void * src, 
    size_t srcLen
);

// Serializes a request and returns the stream id used
int httpRequest (
    HttpConnHandle conn,
    CharBuf * out,
    std::unique_ptr<HttpMsg> msg
);

// Serializes a push promise
void httpPushPromise (
    HttpConnHandle conn,
    CharBuf * out,
    std::unique_ptr<HttpMsg> msg
);

// Serializes a reply on the specified stream
void httpReply (
    HttpConnHandle conn,
    CharBuf * out,
    int stream,
    std::unique_ptr<HttpMsg> msg
);

void httpResetStream (HttpConnHandle conn, CharBuf * out, int stream);

} // namespace

#endif
