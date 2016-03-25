// httptest.cpp - dim test http
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

struct TestMsg {
    vector<NameValue> headers;
    const char * body;
};

struct Test {
    const char * name;
    bool reset;
    string input;
    bool result;
    string output;
    vector<TestMsg> msgs;
};

} // namespace


/****************************************************************************
*     
*   Test vectors
*     
***/  

const Test s_tests[] = {
    {
        "/a",
        true,
        { 
            'P','R','I',' ','*',' ','H','T','T','P','/','2','.','0','\r','\n',
            '\r','\n',
            'S','M','\r','\n',
            '\r','\n',
            0, 0, 0, 4, 0, 0, 0, 0, 0,  // settings
            0, 0, 38, 1, 5, 0, 0, 0, 1, // headers (38 bytes) + eoh + eos
                '\x82',     // :method: GET
                '\x87',     // :scheme: https
                0x44, 0x09, '/','r','e','s','o','u','r','c','e',
                0x66, 0x0b, 'e','x','a','m','p','l','e','.','o','r','g',
                0x53, 0x0a, 'i','m','a','g','e','/','j','p','e','g',
        },
        true,
        { 
            0, 0, 0, 4, 0, 0, 0, 0, 0,  // settings
            0, 0, 0, 4, 1, 0, 0, 0, 0,  // settings + ack
        },
        {
            {
                {
                    { ":method", "GET" },
                    { ":scheme", "https" },
                    { ":path", "/resource" },
                    { "host", "example.org" },
                    { "accept", "image/jpeg" },
                },
                ""
            },
        }
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
    CharBuf output;
    HttpConnHandle conn{};
    bool result;
    vector<unique_ptr<HttpMsg>> msgs;
    for (auto&& test : s_tests) {
        cout << "Test - " << test.name << endl;
        if (test.reset && conn)
            httpClose(conn);
        if (!conn)
            conn = httpListen();
        result = httpRecv(
            conn, 
            &output, 
            &msgs, 
            data(test.input), 
            size(test.input)
        );
        if (result != test.result) {
            logMsgError() << "result: " << result << " != " << test.result 
                 << " (FAILED)";
        }
        if (output.compare(test.output) != 0) 
            logMsgError() << "headers mismatch (FAILED)";
        auto tmi = test.msgs.begin();
        for (auto&& msg : msgs) {
            if (tmi == test.msgs.end()) {
                logMsgError() << "too many messages (FAILED)";
                break;
            }
            if (msg->body().compare(tmi->body) != 0)
                logMsgError() << "body mismatch (FAILED)";
            auto thi = tmi->headers.begin(),
                ethi = tmi->headers.end();
            for (auto&& hdr : *msg) {
                for (auto&& hv : hdr) {
                    if (thi == ethi) {
                        logMsgError() << "expected fewer headers";
                        goto finished_headers;
                    }
                    if (strcmp(thi->name, hdr.m_name) != 0
                        || strcmp(thi->value, hv.m_value) != 0
                    ) {
                        logMsgError() << "header mismatch, '"
                            << hdr.m_name << ": " << hv.m_value 
                            << "', expected '"
                            << thi->name << ": " << thi->value
                            << "' (FAILED)";
                    }
                    ++thi;
                }
            }
        finished_headers:
            if (thi != ethi)
                logMsgError() << "expected more headers (FAILED)";
        }
        msgs.clear();
    }
    httpClose(conn);

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
