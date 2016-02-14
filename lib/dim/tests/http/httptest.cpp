// httptest.cpp - dim test http
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*     
*   Declarations
*     
***/  

namespace {

struct NameValue {
    const char * name;
    const char * value;
    int flags;

    bool operator== (const NameValue & right) const;
};

struct Test {
    const char * name;
    bool reset;
    string input;
    bool result;
    string output;
    vector<NameValue> headers;
    const char * body;
};

} // namespace


/****************************************************************************
*     
*   Test vectors
*     
***/  

const char s_inputA[] =
    "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    "\x00\x00\x00\x04\x00\x00\x00\x00\x00" // settings
    "\x00\x00\x26\x01\x05\x00\x00\x00\x01" // headers (38 bytes) + eoh + eos
        "\x82"                      // :method: GET
        "\x87"                      // :scheme: https
        "\x44\x09" "/resource"      // :path: /resource
        "\x66\x0b" "example.org"    // host: example.org
        "\x53\x0a" "image/jpeg"     // accept: image/jpeg
;
const char s_outputA[] =
    "\x00\x00\x00\x04\x00\x00\x00\x00\x00" // settings
    "\x00\x00\x00\x04\x01\x00\x00\x00\x00" // settings + ack
;

const Test s_tests[] = {
    {
        "/a",
        true,
        { s_inputA, end(s_inputA) - 1 },
        true,
        { s_outputA, end(s_outputA) - 1 },
        {
            { ":method", "GET" },
            { ":scheme", "https" },
            { ":path", "/resource" },
            { "host", "example.org" },
            { "accept", "image/jpeg" },
        },
        ""
    },
};


/****************************************************************************
*     
*   Helpers
*     
***/


/****************************************************************************
*     
*   Logging
*     
***/  

namespace {
class Logger : public IDimLogNotify {
    void OnLog (
        DimLogSeverity severity,
        const string & msg
    ) override;
};
} // namespace

static Logger s_logger;
static int s_errors;

//===========================================================================
void Logger::OnLog (
    DimLogSeverity severity,
    const string & msg
) {
    if (severity >= kError) {
        s_errors += 1;
        cout << "ERROR: " << msg << endl;
    } else {
        cout << msg << endl;
    }
}


/****************************************************************************
*     
*   Reader
*     
***/  


/****************************************************************************
*     
*   External
*     
***/  

int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);
    DimLogRegisterHandler(&s_logger);

    CharBuf output;
    HDimHttpConn conn{};
    bool result;
    for (auto&& test : s_tests) {
        cout << "Test - " << test.name << endl;
        if (test.reset && conn)
            DimHttpClose(conn);
        if (!conn)
            conn = DimHttpListen();
        result = DimHttpRecv(
            conn, 
            NULL, 
            &output, 
            data(test.input), 
            size(test.input)
        );
        if (result != test.result) {
            DimLog{kError} << "result: " << result << " != " << test.result 
                 << " (FAILED)";
        }
        if (output.Compare(test.output) != 0) 
            DimLog{kError} << "headers mismatch (FAILED)";
    }
    DimHttpClose(conn);

    if (s_errors) {
        cout << "*** " << s_errors << " FAILURES" << endl;
        return 1;
    }
    cout << "All tests passed" << endl;
    return 0;
}
