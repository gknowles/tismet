// httpmsg.h - dim core
#ifndef DIM_HTTPMSG_INCLUDED
#define DIM_HTTPMSG_INCLUDED

#include "dim/config.h"

#include <cassert>

enum HttpHdr {
    HTTP_INVALID,
    HTTP_CACHE_CONTROL,
    HTTP_CONNECTION,
    HTTP_CONTENT_ENCODING,
    HTTP_CONTENT_LENGTH,
    HTTP_CONTENT_TYPE,
    HTTP_COOKIE,
    HTTP_DATE,
    HTTP_FORWARDED_FOR,
    HTTP_HOST,
    HTTP_IF_MATCH,
    HTTP_IF_MODIFIED_SINCE,
    HTTP_IF_NONE_MATCH,
    HTTP_LAST_MODIFIED,
    HTTP_SERVER,
    HTTP_SET_COOKIE,
};

class DimHttpMsg {
public:
    struct Hdr;
    class HdrList;
    class HdrIterator;

public:
    const char * Method () const;
    const char * Scheme () const;
    const char * Authority () const;
    const char * Path () const;
    const char * Fragment () const;
    
    int Status () const;

    const Hdr * FirstHeader (int header) const;
    Hdr * FirstHeader (int header);
    const Hdr * LastHeader (int header) const;
    Hdr * LastHeader (int header);
    const Hdr * Next (const Hdr * hdr) const;
    Hdr * Next (Hdr * hdr);
    const Hdr * Prev (const Hdr * hdr) const;
    Hdr * Prev (Hdr * hdr);

    HdrList Headers (
        int header = HTTP_INVALID  // defaults to all
    ) const;

    CharBuf * Data ();
    const CharBuf * Data () const;
};

struct DimHttpMsg::Hdr {
    const char * name;
    const char * value;
private:
    ~Hdr ();
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

class DimHttpMsg::HdrList {
    DimHttpMsg::HdrList (DimHttpMsg & msg, int header);
    DimHttpMsg::HdrIterator it;

    DimHttpMsg::HdrIterator begin () { return it; }
    DimHttpMsg::HdrIterator end () { return DimHttpMsg::HdrIterator(); }
};

class DimHttp2Processer {
    // Recv returns with zero or more requests, push promises, and replies
    bool RecvHeaders (
        std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
        CharBuf * reply,
        int * used, 
        void * src, 
        int srcLen
    );
    bool Recv (
        std::list<std::unique_ptr<DimHttpMsg>> * msg, 
        CharBuf * reply,
        int * used, 
        void * src, 
        int srcLen
    );

    // Serializes a request and returns the stream id used
    int Request (
        CharBuf * out,
        std::unique_ptr<DimHttpMsg> msg
    );

    // Serializes a push promise
    void Push (
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
};

#endif
