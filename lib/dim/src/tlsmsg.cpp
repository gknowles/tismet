// tlsmsg.cpp - dim services
#include "pch.h"
#pragma hdrstop

#include <functional>

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/


/****************************************************************************
*
*   Declarations
*
***/

/****************************************************************************
*
*   Update message components
*
***/

//===========================================================================
void tlsSetKeyShare (TlsKeyShare & out, TlsNamedGroup group) {
    assert(group == kGroupX25519);
    out.keyExchange.assign(32, 0);
}


/****************************************************************************
*
*   Write message components
*
***/

//===========================================================================
static void write (
    TlsRecordWriter & out, 
    const std::vector<TlsKeyShare> & keys
) {
    if (keys.empty())
        return;

    out.number16(kSupportedGroups); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // supported_groups
    out.start16();
    for (auto&& key : keys) {
        out.number16(key.group);
    }
    out.end();
    out.end(); // extension_data

    out.number16(kKeyShare); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // client_shares
    out.start16();
    for (auto&& key : keys) {
        out.number16(key.group); // client_shares.group
        out.start16(); // client_shares.key_exchange
        out.var(key.keyExchange.data(), key.keyExchange.size());
        out.end();
    }
    out.end();
    out.end(); // extension_data
}

//===========================================================================
static void write (TlsRecordWriter & out, const TlsKeyShare & key) {
}

//===========================================================================
static void write (
    TlsRecordWriter & out, 
    const std::vector<TlsPresharedKey> & keys
) {
}

//===========================================================================
static void write (TlsRecordWriter & out, const TlsPresharedKey & key) {
}

//===========================================================================
static void write (
    TlsRecordWriter & out, 
    const std::vector<TlsSignatureScheme> & schemes
) {
    if (schemes.empty())
        return;

    out.number16(kSignatureAlgorithms); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // supported_signature_algorithms
    out.start16();
    for (auto&& sch : schemes) {
        out.number16(sch);
    }
    out.end();
    out.end(); // extension_data
}

//===========================================================================
static void writeSni (TlsRecordWriter & out, const vector<uint8_t> & host) {
    if (host.empty())
        return;

    out.number16(kServerName); // extensions.extension_type
    out.start16(); // extensions.extension_data
    // server_name_list
    out.start16();
    enum TlsNameType : uint8_t {
        kHostName = 0,
    };
    out.number(kHostName);
    out.var16(host.data(), host.size());
    out.end();
    out.end(); // extension_data
}

//===========================================================================
static void writeDraftVersion (TlsRecordWriter & out, uint16_t version) {
    if (!version)
        return;

    out.number16(kDraftVersion);
    out.start16(); // extensions.extension_data
    out.number16(version);
    out.end(); // extension_data
};


/****************************************************************************
*
*   Write messages
*
***/

//===========================================================================
void tlsWrite (TlsRecordWriter & out, const TlsClientHelloMsg & msg) {
    out.contentType(kContentHandshake);
    out.number(kClientHello); // handshake.msg_type
    out.start24(); // handshake.length

    // client_hello
    out.number(msg.majorVersion);
    out.number(msg.minorVersion);
    out.fixed(msg.random, size(msg.random));
    out.number(0); // legacy_session_id
    out.start16(); // cipher_suites
    for (auto&& suite : msg.suites)
        out.number16(suite);
    out.end();
    out.start(); // legacy_compression_methods
    out.number(0);
    out.end();

    out.start16(); // extensions
    write(out, msg.groups);
    write(out, msg.identities);
    write(out, msg.sigSchemes);
    writeSni(out, msg.hostName);
    writeDraftVersion(out, msg.draftVersion);
    out.end(); // extensions

    out.end(); // handshake
}

//===========================================================================
void tlsWrite (TlsRecordWriter & out, const TlsServerHelloMsg & msg) {
    out.contentType(kContentHandshake);
    out.number(kServerHello); // handshake.msg_type
    out.start24(); // handshake.length

    // server_hello
    out.number(msg.majorVersion);
    out.number(msg.minorVersion);
    out.fixed(msg.random, size(msg.random));
    out.number16(msg.suite);

    out.start16(); // extensions
    writeDraftVersion(out, msg.draftVersion);
    write(out, msg.keyShare);
    write(out, msg.identity);
    out.end(); // extensions

    out.end(); // handshake
}

//===========================================================================
void tlsWrite (TlsRecordWriter & out, const TlsHelloRetryRequestMsg & msg) {
    out.contentType(kContentHandshake);
    out.number(kHelloRetryRequest); // handshake.msg_type
    out.start24(); // handshake.length

    // server_hello
    out.number(msg.majorVersion);
    out.number(msg.minorVersion);
    out.number16(msg.suite);
    out.number16(msg.group);

    out.start16(); // extensions
    writeDraftVersion(out, msg.draftVersion);
    out.end(); // extensions

    out.end(); // handshake
}


/****************************************************************************
*
*   Parse message components
*
***/

//===========================================================================
static bool parseSni (
    std::vector<uint8_t> * out,
    TlsRecordReader & in,
    size_t len
) {
    return false;
}

//===========================================================================
static bool parseGroups (
    vector<TlsKeyShare> * out,
    TlsRecordReader & in,
    size_t len
) {
    return false;
}

//===========================================================================
static bool parse (
    vector<TlsSignatureScheme> * out,
    TlsRecordReader & in,
    size_t len
) {
    return false;
}

//===========================================================================
static bool parseKeyShares (
    vector<TlsKeyShare> * out,
    TlsRecordReader & in,
    size_t len
) {
    return false;
}

//===========================================================================
static bool parseDraftVersion (
    uint16_t * out,
    TlsRecordReader & in,
    size_t len
) {
    return false;
}

//===========================================================================
static bool parseExt (
    TlsClientHelloMsg * msg,
    TlsRecordReader & in,
    TlsExtensionType type,
    size_t len
) {
    switch (type) {
    case kServerName:
        return parseSni(&msg->hostName, in, len);
    case kSupportedGroups:
        return parseGroups(&msg->groups, in, len);
    case kSignatureAlgorithms:
        return parse(&msg->sigSchemes, in, len);
    case kKeyShare:
        return parseKeyShares(&msg->groups, in, len);
    case kDraftVersion:
        return parseDraftVersion(&msg->draftVersion, in, len);
    }
    in.skip(len);
    return true;
}

//===========================================================================
static bool parseExt (
    TlsServerHelloMsg * msg,
    TlsRecordReader & in,
    TlsExtensionType type,
    size_t len
) {
    switch (type) {
    case kDraftVersion:
        return parseDraftVersion(&msg->draftVersion, in, len);
    }
    in.skip(len);
    return true;
}

//===========================================================================
template <typename T>
static bool parseExts (T * msg, TlsRecordReader & in) {
    // no extensions? done
    if (!in.size())
        return true;

    size_t extLen = in.number16();
    while (extLen) {
        auto ext = in.number<TlsExtensionType>();
        auto len = in.number16();
        extLen -= len;
        if (!parseExt(msg, in, ext, len))
            return false;
    }
    return true;
}


/****************************************************************************
*
*   Parse messages
*
***/

//===========================================================================
bool tlsParse (TlsClientHelloMsg * msg, TlsRecordReader & in) {
    msg->majorVersion = in.number();
    msg->minorVersion = in.number();
    in.fixed(msg->random, size(msg->random));

    // legacy_session_id
    size_t len = in.number(); 
    if (len > 32) {
        in.setAlert(kDecodeError);
        // TODO: what's the right error?
    }
    in.skip(len);

    // cipher_suites
    len = in.number16();
    if (len % 2) {
        in.setAlert(kDecodeError);
        return false;
    }
    len /= 2;
    for (unsigned i = 0; i < len; ++i) {
        msg->suites.push_back(in.number<TlsCipherSuite>());
    }

    // legacy_compression_methods
    len = in.number();
    if (len != 1 || in.number() != 0) {
        // MUST contain one byte set to zero (null compression)
        in.setAlert(kIllegalParameter);
        return false;
    }

    return parseExts(msg, in);
}

//===========================================================================
bool tlsParse (TlsServerHelloMsg * msg, TlsRecordReader & in) {
    msg->majorVersion = in.number();
    msg->minorVersion = in.number();
    in.fixed(msg->random, size(msg->random));
    msg->suite = in.number<TlsCipherSuite>();

    return parseExts(msg, in);
}

} // namespace
