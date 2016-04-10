// tlsrecord.h - dim services
//
// implements tls/1.3 record protocol, as defined by:
//  rfc5246bis - Transport Layer Security (TLS) Protocol Version 1.3
#pragma once

#include "dim/config.h"

#include <cstdint>
#include <vector>

namespace Dim {

/****************************************************************************
*
*   Constants
*
***/

enum TlsContentType : uint8_t {
    kContentAlert = 21,
    kContentHandshake = 22,
    kContentAppData = 23,
};


/****************************************************************************
*
*   Tls connection
*
***/

struct TlsCipher {
    std::vector<uint8_t> m_key;
    std::vector<uint8_t> m_iv;
    uint64_t m_seq{0};

    virtual ~TlsCipher ();
    virtual void add (CharBuf * out, const void * ptr, size_t count) = 0;
};

class TlsRecordEncrypt {
public:
    void setCipher (CharBuf * out, TlsCipher * cipher);

    void add (
        CharBuf * out,
        TlsContentType ct, 
        const void * ptr, 
        size_t count
    );
    void flush (CharBuf * out);

private:
    void writePlaintext (CharBuf * out);
    void writeCiphertext (CharBuf * out);

    TlsCipher * m_cipher{nullptr};  // cipher if encrypting
    unsigned m_plainType{256};    // content type being encrypted
    std::vector<uint8_t> m_plaintext;   // pending plaintext if encrypting
};


class ITlsRecordDecryptNotify {
public:
    virtual ~ITlsRecordDecryptNotify () {}
    virtual void onTlsAlert (
        TlsAlertDesc desc,
        TlsAlertLevel level
    ) = 0;
    virtual void onTlsHandshake (
        TlsHandshakeType type,
        const uint8_t msg[], 
        size_t msgLen
    ) = 0;
};

class TlsRecordDecrypt {
public:
    void setCipher (TlsCipher * cipher);

    // on error parse(...) returns false and sets alert level and desc 
    // that should be sent to the peer.
    bool parse (
        CharBuf * data, // decrypted application data 
        ITlsRecordDecryptNotify * notify,
        const void * src, 
        size_t srcLen
    );

    // alert level and desc are set when parse(...) returns false
    TlsAlertLevel alertLevel () const { return m_alertLevel; }
    TlsAlertDesc alertDesc () const { return m_alertDesc; }

private:
    bool parseAlerts (ITlsRecordDecryptNotify * notify);
    bool parseHandshakes (ITlsRecordDecryptNotify * notify);
    bool parseError (TlsAlertDesc desc, TlsAlertLevel level = kFatal);

    unsigned m_curType{256};    // content type being decrypted
    unsigned m_recPos{0};
    unsigned m_textLen;
    TlsCipher * m_cipher{nullptr};  // cipher if decrypting
    std::vector<uint8_t> m_ciphertext;  // pending ciphertext if decrypting
    CharBuf m_plaintext;   // pending text (incomplete record)
    TlsAlertLevel m_alertLevel{kFatal};
    TlsAlertDesc m_alertDesc{kInternalError};
};

} // namespace
