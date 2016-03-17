// tls.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private
*
***/

namespace {

class ConnBase : public ITlsRecordDecryptNotify {
public:
    ConnBase ();
    void setSuites (const TlsCipherSuite suites[], size_t count);
    const vector<TlsCipherSuite> & suites () const;

    // ITlsRecordDecryptNotify
    virtual void onTlsAlert (TlsAlertLevel level, TlsAlertDesc desc) override;
    virtual void onTlsHandshake (
        TlsHandshakeType type,
        const uint8_t msg[], 
        size_t msgLen
    ) override = 0;
    virtual void onTlsAppData (const CharBuf & buf) override = 0;

private:
    friend class Writer;
    std::vector<TlsCipherSuite> m_suites;

    TlsContentType m_outType;
    CharBuf m_outbuf;
    struct Pos {
        size_t pos;
        uint8_t bytes;
    };
    std::vector<Pos> m_outStack;
    CharBuf * m_out;
    TlsRecordEncrypt m_encrypt;
    
    TlsRecordDecrypt m_in;
};

class Writer {
public:
    Writer (ConnBase * rec, CharBuf * out);
    ~Writer ();

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


class ClientConn : public ConnBase {
public:
    void connect (CharBuf * out);

private:
    void onTlsHandshake (
        TlsHandshakeType type,
        const uint8_t msg[], 
        size_t msgLen
    ) override;
    void onTlsAppData (const CharBuf & buf) override;
};

class ServerConn : public ConnBase {
public:

private:
    void onTlsHandshake (
        TlsHandshakeType type,
        const uint8_t msg[], 
        size_t msgLen
    ) override;
    void onTlsAppData (const CharBuf & buf) override;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TlsConnHandle, ConnBase> s_conns;


/****************************************************************************
*
*   ConnBase
*
***/

//===========================================================================
ConnBase::ConnBase () 
{}

//===========================================================================
void ConnBase::setSuites (const TlsCipherSuite suites[], size_t count) {
    m_suites.assign(suites, suites + count);
    sort(m_suites.begin(), m_suites.end());
    auto last = unique(m_suites.begin(), m_suites.end());
    m_suites.erase(last, m_suites.end());
}

//===========================================================================
const std::vector<TlsCipherSuite> & ConnBase::suites () const {
    return m_suites;
}

//===========================================================================
void ConnBase::onTlsAlert (TlsAlertLevel level, TlsAlertDesc desc) {
}


/****************************************************************************
*
*   Writer
*
***/

//===========================================================================
Writer::Writer (ConnBase * conn, CharBuf * out)
    : m_rec(conn->m_encrypt)
    , m_out(out)
{}

//===========================================================================
Writer::~Writer () {
    if (size_t count = m_buf.size()) {
        assert(m_stack.empty());
        m_rec.add(m_out, (TlsContentType) m_type, m_buf.data(), count);
    }
}

//===========================================================================
void Writer::contentType (TlsContentType type) {
    m_type = type;
}

//===========================================================================
void Writer::number (uint8_t val) {
    fixed(&val, 1);
}

//===========================================================================
void Writer::number16 (uint16_t val) {
    uint8_t buf[2] = { uint8_t(val >> 8), uint8_t(val) };
    fixed(buf, size(buf));
}

//===========================================================================
void Writer::fixed (const void * ptr, size_t count) {
    if (m_buf.size()) {
        m_buf.append((const char *) ptr, count);
    } else {
        m_rec.add(m_out, (TlsContentType) m_type, ptr, count);
    }
}

//===========================================================================
void Writer::var (const void * ptr, size_t count) {
    assert(count < 1 << 8);
    number((uint8_t) count);
    fixed(ptr, count);
}

//===========================================================================
void Writer::var16 (const void * ptr, size_t count) {
    assert(count < 1 << 16);
    number16((uint16_t) count);
    fixed(ptr, count);
}

//===========================================================================
void Writer::start () {
    m_stack.push_back({m_buf.size(), 1});
    m_buf.append(1, 0);
}

//===========================================================================
void Writer::start16 () {
    m_stack.push_back({m_buf.size(), 2});
    m_buf.append(2, 0);
}

//===========================================================================
void Writer::start24 () {
    m_stack.push_back({m_buf.size(), 3});
    m_buf.append(3, 0);
}

//===========================================================================
void Writer::end () {
    Pos & pos = m_stack.back();
    size_t count = m_buf.size() - pos.pos;
    char buf[4];
    switch (pos.width) {
        default: assert(0);
        case 3: buf[1] = uint8_t(count >> 16);
        case 2: buf[2] = uint8_t(count >> 8);
        case 1: buf[3] = uint8_t(count);
    };
    m_buf.replace(pos.pos, pos.width, buf + 4 - pos.width, pos.width);
    m_stack.pop_back();
    if (!m_stack.size()) {
        m_rec.add(
            m_out, 
            (TlsContentType) m_type, 
            m_buf.data(),
            m_buf.size()
        );
        m_buf.clear();
    }
}


/****************************************************************************
*
*   ClientConn
*
***/

const uint8_t kClientVersion[] = { 3, 4 };

//===========================================================================
void ClientConn::connect (CharBuf * outbuf) {
    Writer out(this, outbuf);

    out.contentType(kContentHandshake);
    out.number(kClientHello); // handshake.msg_type
    out.start24(); // handshake.length

    // client_hello
    out.fixed(kClientVersion, size(kClientVersion)); // client_version
    uint8_t random[32];
    out.fixed(random, size(random));
    out.number(0); // legacy_session_id
    out.start16(); // cipher_suites
    for (auto&& suite : suites())
        out.number16(suite);
    out.end();
    out.start(); // legacy_compression_methods
    out.number(0);
    out.end();

    out.start16(); // extensions

    out.number16(kKeyShare); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // client_shares
    out.start16();
    out.number16(kEddsaEd25519); // client_shares.group
    out.start16(); // client_shares.key_exchange
    uint8_t point[32];
    out.var(point, size(point)); // point
    out.end();
    out.end();
    out.end(); // extension_data

    out.number16(kSignatureAlgorithms); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // supported_signature_algorithms
    out.start16();
    out.number(4); // hash (sha256)
    out.number(5); // signature (eddsa)
    out.end();
    out.end(); // extension_data

    out.end(); // extensions

    out.end(); // handshake
}

//===========================================================================
void ClientConn::onTlsHandshake (
    TlsHandshakeType type,
    const uint8_t msg[], 
    size_t msgLen
) {
}

//===========================================================================
void ClientConn::onTlsAppData (const CharBuf & buf) {
}


/****************************************************************************
*
*   ServerConn
*
***/

//===========================================================================
void ServerConn::onTlsHandshake (
    TlsHandshakeType type,
    const uint8_t msg[], 
    size_t msgLen
) {
}

//===========================================================================
void ServerConn::onTlsAppData (const CharBuf & buf) {
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
TlsConnHandle tlsAccept (
    const TlsCipherSuite suites[], 
    size_t count
) {
    auto conn = new ServerConn;
    conn->setSuites(suites, count);
    return s_conns.insert(conn);
}

//===========================================================================
TlsConnHandle tlsConnect (
    CharBuf * out,
    const TlsCipherSuite suites[], 
    size_t count
) {
    auto conn = new ClientConn;
    conn->setSuites(suites, count);
    conn->connect(out);
    return s_conns.insert(conn);
}

//===========================================================================
void tlsClose (TlsConnHandle h) {
    s_conns.erase(h);
}

//===========================================================================
bool tlsRecv (
    TlsConnHandle conn,
    CharBuf * out,
    CharBuf * plain,
    const void * src,
    size_t srcLen
) {
    return false;
}


} // namespace
