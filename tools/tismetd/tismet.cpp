// main.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   EndpointFind
*
***/

class EndpointFind : public IEndpointNotify {
    void onEndpointFound (Endpoint * ptr, int count) override;
};
static EndpointFind s_endFind;

//===========================================================================
void EndpointFind::onEndpointFound (Endpoint * ptr, int count) {
    cout << "\nDNS Addresses:" << endl;
    for (int i = 0; i < count; ++i) {
        cout << ptr[i] << endl;
    }
    // DimAppSignalShutdown(9);
}


/****************************************************************************
*
*   ListenSocket
*
***/

class ListenSocket : public ISocketNotify {
    void onSocketAccept (const SocketAcceptInfo & info) override;
    void onSocketDisconnect () override;
    void onSocketRead (const SocketData & data) override;

    HttpConnHandle m_conn;
};

//===========================================================================
void ListenSocket::onSocketAccept (const SocketAcceptInfo & info) {
    cout << "\n*** ACCEPTED " << info.remoteEnd << " to " 
        << info.localEnd << endl;

    m_conn = httpListen();
}

//===========================================================================
void ListenSocket::onSocketDisconnect () {
    cout << "\n*** DISCONNECTED" << endl;

    httpClose(m_conn);
}

//===========================================================================
void ListenSocket::onSocketRead (const SocketData & data) {
    //list<unique_ptr<HttpMsg>> msgs;
    //CharBuf out;
    //bool disconnect = !httpRecv(
    //    m_conn,
    //    &msgs, 
    //    &out,
    //    data.data,
    //    data.bytes
    //);
    //unique_ptr<SocketBuffer> sbuf = socketGetBuffer();
    //socketWrite(this, move(sbuf), 0);
    //if (disconnect)
    //    socketDisconnect(this);

    cout.write(data.data, data.bytes);
    cout.flush();
}


/****************************************************************************
*
*   ListenNotify
*
***/

struct ListenNotify : ISocketListenNotify {
    void onListenStop (const Endpoint & local) override;
    unique_ptr<ISocketNotify> onListenCreateSocket (
        const Endpoint & local
    ) override;
};
static ListenNotify s_listen;

//===========================================================================
void ListenNotify::onListenStop (const Endpoint & local) {
    cout << "*** STOPPED LISTENING" << endl;
}

//===========================================================================
unique_ptr<ISocketNotify> ListenNotify::onListenCreateSocket (
    const Endpoint & local
) {
    return make_unique<ListenSocket>();
}


/****************************************************************************
*
*   MainShutdown
*
***/

class MainShutdown : public IAppShutdownNotify {
    void onAppStartClientCleanup () override;
    bool onAppQueryClientDestroy () override;
};
static MainShutdown s_cleanup;

//===========================================================================
void MainShutdown::onAppStartClientCleanup () {
}

//===========================================================================
bool MainShutdown::onAppQueryClientDestroy () {
    return true;
}


/****************************************************************************
*     
*   Application
*     
***/

namespace {
class Application : public IAppNotify {
    void onAppRun () override;
};
} // namespace

//===========================================================================
void Application::onAppRun () {
    appMonitorShutdown(&s_cleanup);

    vector<Address> addrs;
    addressGetLocal(&addrs);
    cout << "Local Addresses:" << endl;
    for (auto&& addr : addrs) {
        cout << addr << endl;
    }

    Endpoint end;
    parse(&end, "127.0.0.1", 8888);
    socketListen(&s_listen, end);

    //HttpConn context;
    //std::vector<std::unique_ptr<HttpMsg>> msgs;
    //CharBuf reply;
    //context.recv(&msgs, &reply, NULL, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 24);

    //socketStop(nullptr, Endpoint{});

    //if (argc > 1) {
    //    int cancelId;
    //    endpointQuery(&cancelId, &s_endFind, argv[1], 0);
    //} else {
    //    appSignalShutdown(8);
    //}
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);

    Application app;
    int code = appRun(app, argc, argv);
    return code;
}
