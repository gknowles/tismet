// tlstest.cpp - dim test tls
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*     
*   Declarations
*     
***/  

namespace {

enum {
    kTestReset = 0x01,
    kTestClient = 0x02,
};

struct Test {
    const char * name;
    unsigned flags; // kTest*
    string input;
    bool result;
    string output;
    string appdata;
};

} // namespace


/****************************************************************************
*     
*   Test vectors
*     
***/  

const Test s_tests[] = {
    {
        "connect",
        kTestReset | kTestClient,
        {},
        true,
        {},
        {}
    },
    {
        "simple client hello",
        kTestReset,
        { 
            kClientHello, // msg_type
            0, 0, 10, // length
            // ClientHello
            3, 4, // client_version
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // random[32]
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            0, // legacy_session_id<0..32>
            0, 2, '\xcc', '\xa9', // cipher_suites<2..2^16-2>
            0, 0, // legacy_compression_methods<1..2*8-1> (null compression)
            0, 49, // extensions<0..2^16-1>
                // KeyShare
                '\x88', 2,    // extension type
                0, 37, // extension_data<0..2^16-1>
                    0, 29, // group (kEcdhX25519)
                    0, 33, // key_exchange<1..2^16-1>
                        32, // point<1..2^8-1>
                            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                // SignatureAndHashAlgorithm
                0, 13,  // extension type
                0, 4, // extension_data<0..2^16-1>
                    0, 2, // supported_signature_algorithms<2..2^16-2>
                        4, 5, // (sha256, eddsa)
        },
        true,
        { 
            // ServerHello
            // EncryptedExtensions
            // CertificateRequest
            // ServerConfiguration
        },
        {}
    },
};


/****************************************************************************
*     
*   Helpers
*     
***/


/****************************************************************************
*     
*   Application
*     
***/

namespace {

class Application 
    : public ITaskNotify 
    , public ILogNotify
{
    // ITaskNotify
    void onTask () override;

    // ILogNotify
    void onLog (LogType type, const string & msg) override;

    int m_errors;
};

} // namespace

//===========================================================================
void Application::onLog (LogType type, const string & msg) {
    if (type >= kLogError) {
        m_errors += 1;
        cout << "ERROR: " << msg << endl;
    } else {
        cout << msg << endl;
    }
}

//===========================================================================
void Application::onTask () {
    const TlsCipherSuite kCiphers[] = { 
        TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 
    };
    const char kHost[] = "example.com";

    CharBuf output;
    CharBuf plain;
    TlsConnHandle client{};
    TlsConnHandle server{};
    bool result;
    for (auto&& test : s_tests) {
        cout << "Test - " << test.name << endl;
        TlsConnHandle & conn = (test.flags & kTestClient) ? client : server;
        if ((test.flags & kTestReset) && conn)
            tlsClose(conn);
        if (!conn) {
            if (test.flags & kTestClient) {
                conn = tlsConnect(&output, kHost, kCiphers, size(kCiphers));
            } else {
                conn = tlsAccept(kCiphers, size(kCiphers));
            }
        }
        result = tlsRecv(
            conn, 
            &output, 
            &plain, 
            data(test.input), 
            size(test.input)
        );
        if (result != test.result) {
            logMsgError() << "result: " << result << " != " << test.result 
                 << " (FAILED)";
        }
        if (output.compare(test.output) != 0) 
            logMsgError() << "headers mismatch (FAILED)";
    }
    tlsClose(client);
    tlsClose(server);

    if (m_errors) {
        cout << "*** " << m_errors << " FAILURES" << endl;
        appSignalShutdown(1);
    } else {
        cout << "All tests passed" << endl;
        appSignalShutdown(0);
    }
}


/****************************************************************************
*     
*   External
*     
***/  

int main (int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);
    Application app;
    logAddNotify(&app);
    return appRun(app);
}
