// tls.h - dim services
//
// implements tls/1.3, as defined by:
//  rfc5246bis - Transport Layer Security (TLS) Protocol Version 1.3
#pragma once

#include "dim/config.h"

#include <cstdint>

namespace Dim {

/****************************************************************************
*
*   Constants
*
***/

enum TlsAlertLevel : uint8_t {
    kWarning = 1,
    kFatal = 2,
};

enum TlsAlertDesc : uint8_t {
    kCloseNotify = 0,
    kEndOfEarlyData = 1,
    kUnexpectedMessage = 10,            // fatal
    kBadRecordMac = 20,                 // fatal
    kRecordOverflow = 22,               // fatal
    kHandshakeFailure = 40,             // fatal
    kBadCertificate = 42,
    kUnsupportedCertificate = 43,
    kCertificateRevoked = 44,
    kCertificateExpired = 45,
    kCertificateUnknown = 46,
    kIllegalParameter = 47,             // fatal
    kUnknownCa = 48,                    // fatal
    kAccessDenied = 49,                 // fatal
    kDecodeError = 50,                  // fatal
    kDecryptError = 51,                 // fatal
    kProtocolVersion = 70,              // fatal
    kInsufficientSecurity = 71,         // fatal
    kInternalError = 80,                // fatal
    kInappropriateFallback = 86,        // fatal
    kUserCanceled = 90,
    kMissingExtension = 109,            // fatal
    kUnsupportedExtension = 110,        // fatal
    kCertificateUnobtainable = 111,
    kUnrecognizedName = 112,
    kBadCertificateStatusResponse = 113,// fatal
    kBadCertificateHashValue = 114,     // fatal
    kUnknownPskIdentity = 115,
};

enum TlsHandshakeType : uint8_t {
    kClientHello = 1,
    kServerHello = 2,
    kSessionTicket = 4,
    kHelloRetryRequest = 6,
    kEncryptedExtensions = 8,
    kCertificate = 11,
    kCertificateRequest = 13,
    kCertificateVerify = 15,
    kServerConfiguration = 17,
    kFinished = 20,
    kKeyUpdate = 24,
};

enum TlsCipherSuite : uint16_t {
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xcca8,
    TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 = 0xcca9,      
    TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xccaa,    
};

enum TlsExtensionType : uint16_t {
    kServerName = 0,
    kSupportedGroups = 10,
    kSignatureAlgorithms = 13,
    kEarlyData = 40,
    kPresharedKey = 41,
    kKeyShare = 42,
    kDraftVersion = 0xff02,
};

enum TlsNamedGroup : uint16_t {
    kGroupX25519 = 29,
};

enum TlsSignatureScheme : uint16_t {
    kSigEd25519 = 0x0703,
};


/****************************************************************************
*
*   Tls connection context
*
***/

struct TlsConnHandle : HandleBase {};

TlsConnHandle tlsConnect (
    CharBuf * out,
    const char hostName[],
    const TlsCipherSuite suites[], 
    size_t count
);
TlsConnHandle tlsAccept (
    const TlsCipherSuite suites[], 
    size_t count
);
void tlsClose (TlsConnHandle h);

bool tlsRecv (
    TlsConnHandle conn,
    CharBuf * out,
    CharBuf * data,
    const void * src,
    size_t srcLen
);

void tlsSend (
    TlsConnHandle conn,
    CharBuf * out,
    const void * src,
    size_t srcLen
);

} // namespace
