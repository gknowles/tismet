// hpacktest.cpp - dim test hpack
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

struct NameValue {
    const char * name;
    const char * value;
    int flags;

    bool operator== (const NameValue & right) const;
};

class Reader : public IHpackDecodeNotify {
public:
    vector<NameValue> headers;

private:
    void onHpackHeader (
        HttpHdr id,
        const char name[],
        const char value[],
        int flags // Flags::*
    ) override;
};

struct Test {
    const char * name;
    bool reset;
    const char * input;
    bool result;
    vector<NameValue> headers;
    vector<NameValue> dynTable;
};

} // namespace


/****************************************************************************
*     
*   Test vectors
*     
***/  

const Test s_tests[] = {
    {   
        "C.2.1",
        true,
        // @.custom-key.cus
        // tom-header
        "\x40\x0a\x63\x75\x73\x74\x6f\x6d\x2d\x6b\x65\x79\x0d\x63\x75\x73"
        "\x74\x6f\x6d\x2d\x68\x65\x61\x64\x65\x72",
        true,
        { 
            { "custom-key", "custom-header" },
        }
    },
    {   
        "C.2.2",
        true,
        // ../sample/path
        "\x04\x0c\x2f\x73\x61\x6d\x70\x6c\x65\x2f\x70\x61\x74\x68",
        true,
        { 
            { ":path", "/sample/path" },
        }
    },
    {   
        "C.2.3",
        true,
        // ..password.secre
        // t
        "\x10\x08\x70\x61\x73\x73\x77\x6f\x72\x64\x06\x73\x65\x63\x72\x65"
        "\x74",
        true,
        { 
            { "password", "secret" },
        }
    },
    {   
        "C.2.4",
        true,
        // .
        "\x82",
        true,
        { 
            { ":method", "GET" },
        }
    },
    {   
        "C.3.1",
        true,
        // ...A.www.example
        // .com
        "\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65"
        "\x2e\x63\x6f\x6d",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "http" },
            { ":path", "/" },
            { ":authority", "www.example.com" },
        },
        {
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.3.2",
        false,
        // ....X.no-cache
        "\x82\x86\x84\xbe\x58\x08\x6e\x6f\x2d\x63\x61\x63\x68\x65",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "http" },
            { ":path", "/" },
            { ":authority", "www.example.com" },
            { "cache-control", "no-cache" },
        },
        {
            { "cache-control", "no-cache" },
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.3.3",
        false,
        // ....@.custom-key
        // .custom-value
        "\x82\x87\x85\xbf\x40\x0a\x63\x75\x73\x74\x6f\x6d\x2d\x6b\x65\x79"
        "\x0c\x63\x75\x73\x74\x6f\x6d\x2d\x76\x61\x6c\x75\x65",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "https" },
            { ":path", "/index.html" },
            { ":authority", "www.example.com" },
            { "custom-key", "custom-value" },
        },
        {
            { "custom-key", "custom-value" },
            { "cache-control", "no-cache" },
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.4.1",
        true,
        // ...A......:k....
        // .
        "\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4"
        "\xff",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "http" },
            { ":path", "/" },
            { ":authority", "www.example.com" },
        },
        {
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.4.2",
        false,
        // ....X....d..
        "\x82\x86\x84\xbe\x58\x86\xa8\xeb\x10\x64\x9c\xbf",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "http" },
            { ":path", "/" },
            { ":authority", "www.example.com" },
            { "cache-control", "no-cache" },
        },
        {
            { "cache-control", "no-cache" },
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.4.3",
        false,
        // ....@.%.I.[.}..%
        // .I.[....
        "\x82\x87\x85\xbf\x40\x88\x25\xa8\x49\xe9\x5b\xa9\x7d\x7f\x89\x25"
        "\xa8\x49\xe9\x5b\xb8\xe8\xb4\xbf",
        true,
        { 
            { ":method", "GET" },
            { ":scheme", "https" },
            { ":path", "/index.html" },
            { ":authority", "www.example.com" },
            { "custom-key", "custom-value" },
        },
        {
            { "custom-key", "custom-value" },
            { "cache-control", "no-cache" },
            { ":authority", "www.example.com" },
        }
    },
    {   
        "C.5.1",
        true,
        // H.302X.privatea.
        // Mon, 21 Oct 2013
        //  20:13:21 GMTn.h
        // ttps://www.examp
        // le.com
        "\x48\x03\x33\x30\x32\x58\x07\x70\x72\x69\x76\x61\x74\x65\x61\x1d"
        "\x4d\x6f\x6e\x2c\x20\x32\x31\x20\x4f\x63\x74\x20\x32\x30\x31\x33"
        "\x20\x32\x30\x3a\x31\x33\x3a\x32\x31\x20\x47\x4d\x54\x6e\x17\x68"
        "\x74\x74\x70\x73\x3a\x2f\x2f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70"
        "\x6c\x65\x2e\x63\x6f\x6d",
        true,
        { 
            { ":status", "302" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "location", "https://www.example.com" },
        },
        {
            { "location", "https://www.example.com" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "cache-control", "private" },
            { ":status", "302" },
        }
    },
    {
        "C.5.2",
        false,
        // H.307...
        "\x48\x03\x33\x30\x37\xc1\xc0\xbf",
        true,
        {
            { ":status", "307" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "location", "https://www.example.com" },
        },
        {
            { ":status", "307" },
            { "location", "https://www.example.com" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "cache-control", "private" },
        }
    },
    {   
        "C.5.3",
        false,
        // ..a.Mon, 21 Oct 
        // 2013 20:13:22 GM
        // T.Z.gzipw8foo=AS
        // DJKHQKBZXOQWEOPI
        // UAXQWEOIU; max-a
        // ge=3600; version
        // =1
        "\x88\xc1\x61\x1d\x4d\x6f\x6e\x2c\x20\x32\x31\x20\x4f\x63\x74\x20"
        "\x32\x30\x31\x33\x20\x32\x30\x3a\x31\x33\x3a\x32\x32\x20\x47\x4d"
        "\x54\xc0\x5a\x04\x67\x7a\x69\x70\x77\x38\x66\x6f\x6f\x3d\x41\x53"
        "\x44\x4a\x4b\x48\x51\x4b\x42\x5a\x58\x4f\x51\x57\x45\x4f\x50\x49"
        "\x55\x41\x58\x51\x57\x45\x4f\x49\x55\x3b\x20\x6d\x61\x78\x2d\x61"
        "\x67\x65\x3d\x33\x36\x30\x30\x3b\x20\x76\x65\x72\x73\x69\x6f\x6e"
        "\x3d\x31",
        true,
        {
            { ":status", "200" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:22 GMT" },
            { "location", "https://www.example.com" },
            { "content-encoding", "gzip" },
            { 
                "set-cookie", 
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1" 
            },
        },
        {
            { 
                "set-cookie", 
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1" 
            },
            { "content-encoding", "gzip" },
            { "date", "Mon, 21 Oct 2013 20:13:22 GMT" },
        }
    },
    {
        "C.6.1",
        true,
        // H.d.X...w.Ka..z.
        // ..T.D. .....f...
        // -..n..)...c.....
        // ....C.
        "\x48\x82\x64\x02\x58\x85\xae\xc3\x77\x1a\x4b\x61\x96\xd0\x7a\xbe"
        "\x94\x10\x54\xd4\x44\xa8\x20\x05\x95\x04\x0b\x81\x66\xe0\x82\xa6"
        "\x2d\x1b\xff\x6e\x91\x9d\x29\xad\x17\x18\x63\xc7\x8f\x0b\x97\xc8"
        "\xe9\xae\x82\xae\x43\xd3",
        true,
        { 
            { ":status", "302" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "location", "https://www.example.com" },
        },
        {
            { "location", "https://www.example.com" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "cache-control", "private" },
            { ":status", "302" },
        }
    },
    {
        "C.6.2",
        false,
        // H.d.....
        "\x48\x83\x64\x0e\xff\xc1\xc0\xbf",
        true,
        {
            { ":status", "307" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "location", "https://www.example.com" },
        },
        {
            { ":status", "307" },
            { "location", "https://www.example.com" },
            { "date", "Mon, 21 Oct 2013 20:13:21 GMT" },
            { "cache-control", "private" },
        }
    },
    {
        "C.6.3",
        false,
        // ..a..z...T.D. ..
        // ...f...-...Z....
        // w..........5...[
        // 9`..'..6r..'..).
        // ..1`e...N...=P.
        "\x88\xc1\x61\x96\xd0\x7a\xbe\x94\x10\x54\xd4\x44\xa8\x20\x05\x95"
        "\x04\x0b\x81\x66\xe0\x84\xa6\x2d\x1b\xff\xc0\x5a\x83\x9b\xd9\xab"
        "\x77\xad\x94\xe7\x82\x1d\xd7\xf2\xe6\xc7\xb3\x35\xdf\xdf\xcd\x5b"
        "\x39\x60\xd5\xaf\x27\x08\x7f\x36\x72\xc1\xab\x27\x0f\xb5\x29\x1f"
        "\x95\x87\x31\x60\x65\xc0\x03\xed\x4e\xe5\xb1\x06\x3d\x50\x07",
        true,
        {
            { ":status", "200" },
            { "cache-control", "private" },
            { "date", "Mon, 21 Oct 2013 20:13:22 GMT" },
            { "location", "https://www.example.com" },
            { "content-encoding", "gzip" },
            { 
                "set-cookie", 
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1" 
            },
        },
        {
            { 
                "set-cookie", 
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1" 
            },
            { "content-encoding", "gzip" },
            { "date", "Mon, 21 Oct 2013 20:13:22 GMT" },
        }
    },
};


/****************************************************************************
*     
*   Helpers
*     
***/

//===========================================================================
bool NameValue::operator== (const NameValue & right) const {
    return strcmp(name, right.name) == 0
        && strcmp(value, right.value) == 0
        && flags == right.flags;
}


/****************************************************************************
*     
*   Reader
*     
***/  

//===========================================================================
void Reader::onHpackHeader (
    HttpHdr id,
    const char name[],
    const char value[],
    int flags // Flags::*
) {
    cout << name << ": " << value << "\n";
    headers.push_back({name, value, flags});
}


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
    TempHeap heap;
    HpackDecode decode(256);
    Reader out;
    bool result;
    for (auto&& test : s_tests) {
        cout << "Test - " << test.name << endl;
        out.headers.clear();
        if (test.reset) 
            decode.reset();
        size_t srcLen = strlen(test.input);
        result = decode.parse(&out, &heap, test.input, srcLen);
        if (result != test.result) {
            logMsgError() << "result: " << result << " != " << test.result 
                 << " (FAILED)";
        }
        if (test.headers != out.headers)
            logMsgError() << "headers mismatch (FAILED)";
        //if (test.dynTable != decode.DynamicTable())
        //    cout << "dynamic table mismatch (FAILED)" << endl;
    }

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

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);
    Application app;
    logAddNotify(&app);
    return appRun(app);
}
