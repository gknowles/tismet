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

class DimHttpMsg {
public:
    struct Hdr;
    struct HdrValue;
    class HdrRange;
    class HdrIterator;

public:
    virtual ~DimHttpMsg () {}

    const Hdr * FindFirst (int header) const;
    Hdr * FindFirst (int header);
    const Hdr * FindLast (int header) const;
    Hdr * FindLast (int header);
    const Hdr * Next (const Hdr * hdr) const;
    Hdr * Next (Hdr * hdr);
    const Hdr * Prev (const Hdr * hdr) const;
    Hdr * Prev (Hdr * hdr);

    void AddHeader (HttpHdr id, const char value[]);
    void AddHeader (const char name[], const char value[]);

    // When adding references the memory referenced by the name and value 
    // pointers must be valid for the life of the http msg, such as constants 
    // or strings allocated from this messages Heap().
    void AddHeaderRef (HttpHdr id, const char value[]);
    void AddHeaderRef (const char name[], const char value[]);

    HdrRange Headers (
        int header = kHttpInvalid // defaults to all
    ) const;

    CharBuf * Body ();
    const CharBuf * Body () const;

    IDimTempHeap & Heap ();

protected:
    virtual bool CheckPseudoHeaders () const = 0;

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
    DimTempHeap m_heap;
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


class DimHttpRequestMsg : public DimHttpMsg {
public:
    const char * Method () const;
    const char * Scheme () const;
    const char * Authority () const;

    // includes path, query, and fragment
    const char * PathAbsolute () const;

    const char * Path () const;
    const char * Query () const;
    const char * Fragment () const;
    
private:
    bool CheckPseudoHeaders () const override;
};

class DimHttpResponseMsg : public DimHttpMsg {
public:
    int Status () const;

private:
    bool CheckPseudoHeaders () const override;
};


/****************************************************************************
*
*   Http connection context
*
***/

struct HDimHttpConn : DimHandleBase {};

HDimHttpConn DimHttpConnect (CharBuf * out);
HDimHttpConn DimHttpListen ();

void DimHttpClose (HDimHttpConn conn);

// Returns false when no more data will be accepted, either by request
// of the input or due to error.
// Even after an error, msgs and out should be processed.
//  - msg: zero or more requests, push promises, and/or replies are appended
//  - out: data to send to the remote endpoint is appended
bool DimHttpRecv (
    HDimHttpConn conn,
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * out,
    const void * src, 
    size_t srcLen
);

// Serializes a request and returns the stream id used
int DimHttpRequest (
    HDimHttpConn conn,
    CharBuf * out,
    std::unique_ptr<DimHttpMsg> msg
);

// Serializes a push promise
void DimHttpPushPromise (
    HDimHttpConn conn,
    CharBuf * out,
    std::unique_ptr<DimHttpMsg> msg
);

// Serializes a reply on the specified stream
void DimHttpReply (
    HDimHttpConn conn,
    CharBuf * out,
    int stream,
    std::unique_ptr<DimHttpMsg> msg
);

void DimHttpResetStream (HDimHttpConn conn, CharBuf * out, int stream);


#endif
