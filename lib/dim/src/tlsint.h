// tlsint.h - dim core
#pragma once

#include <vector>

namespace Dim {


class TlsRecordReader;
class TlsRecordWriter;

/****************************************************************************
*
*   Handshake messages
*
***/

struct TlsKeyShare {
    std::vector<uint8_t> keyExchange;
    TlsNamedGroup group;
};

struct TlsPresharedKey {
    std::vector<uint8_t> data;
};

struct TlsClientHelloMsg {
    uint8_t majorVersion;
    uint8_t minorVersion;
    uint8_t random[32];
    std::vector<TlsCipherSuite> suites;

    // extensions
    uint16_t draftVersion;
    std::vector<TlsKeyShare> groups;
    std::vector<TlsPresharedKey> identities;
    std::vector<TlsSignatureScheme> sigSchemes;
    std::vector<uint8_t> hostName;
};

struct TlsServerHelloMsg {
    uint8_t majorVersion;
    uint8_t minorVersion;
    uint8_t random[32];
    TlsCipherSuite suite;

    // extensions
    uint16_t draftVersion;
    TlsKeyShare keyShare;
    TlsPresharedKey identity;
};

struct TlsHelloRetryRequestMsg {
    uint8_t majorVersion;
    uint8_t minorVersion;
    TlsCipherSuite suite;
    TlsNamedGroup group;

    // extensions
    uint16_t draftVersion;
};

struct TlsEncryptedExtensionsMsg {
};


void tlsSetKeyShare (TlsKeyShare & out, TlsNamedGroup group);

void tlsWrite (TlsRecordWriter & out, const TlsClientHelloMsg & msg);
void tlsWrite (TlsRecordWriter & out, const TlsServerHelloMsg & msg);

bool tlsParse (TlsClientHelloMsg * msg, TlsRecordReader & in);
bool tlsParse (TlsServerHelloMsg * msg, TlsRecordReader & in);


/****************************************************************************
*
*   Tls connection
*
***/

class TlsConnBase : public ITlsRecordDecryptNotify {
public:
    TlsConnBase ();
    void setSuites (const TlsCipherSuite suites[], size_t count);
    const std::vector<TlsCipherSuite> & suites () const;

    bool recv (
        CharBuf * reply,
        CharBuf * data,
        const void * src,
        size_t srcLen
    );

    // ITlsRecordDecryptNotify
    virtual void onTlsAlert (TlsAlertDesc desc, TlsAlertLevel level) override;
    virtual void onTlsHandshake (
        TlsHandshakeType type,
        const uint8_t msg[], 
        size_t msgLen
    ) override;

    virtual void onTlsHandshake (const TlsClientHelloMsg & msg);
    virtual void onTlsHandshake (const TlsServerHelloMsg & msg);
    virtual void onTlsHandshake (const TlsHelloRetryRequestMsg & msg);

private:
    friend class TlsRecordWriter;
    friend class TlsRecordReader;
    void addAlert (TlsAlertDesc desc, TlsAlertLevel level = kFatal);

    template <typename T>
    void handshake (TlsRecordReader & in);

    std::vector<TlsCipherSuite> m_suites;

    CharBuf * m_reply{nullptr};
    TlsRecordEncrypt m_encrypt;
    TlsRecordDecrypt m_in;
};

class TlsRecordWriter {
public:
    TlsRecordWriter (TlsConnBase & conn, CharBuf * out);
    ~TlsRecordWriter ();

    void contentType (TlsContentType type);

    void number (uint8_t val);
    void number16 (uint16_t val);
    void fixed (const void * ptr, size_t count);

    // Complete variable length vector
    void var (const void * ptr, size_t count);
    void var16 (const void * ptr, size_t count);

    // Variable length vector. Start the vector, use number and fixed to set
    // the content, and then end the vector. May be nested.
    void start ();
    void start16 ();
    void start24 ();
    void end ();

private:
    CharBuf * m_out{nullptr};
    TlsRecordEncrypt & m_rec;

    unsigned m_type{256};
    CharBuf m_buf;
    struct Pos {
        size_t pos;
        uint8_t width;
    };
    std::vector<Pos> m_stack;
};

class TlsRecordReader {
public:
    TlsRecordReader (TlsConnBase & conn, const void * ptr, size_t count);

    uint8_t number ();
    uint16_t number16 ();
    unsigned number24 ();
    template <typename T> T number ();
    void fixed (uint8_t * dst, size_t dstLen);
    void skip (size_t count);

    void setAlert (TlsAlertDesc desc, TlsAlertLevel level = kFatal);

    size_t size () const;
    
private:
    bool m_failed{false};
    TlsConnBase & m_conn;
    const uint8_t * m_ptr;
    int m_count;
};

template <typename T>
inline T TlsRecordReader::number () {
    switch (sizeof(T)) {
        case 1: return (T) number();
        case 2: return (T) number16();
    }
    assert(sizeof(T) == 3);
    return (T) number24();
}


} // namespace
