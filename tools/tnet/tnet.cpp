// main.cpp - tnet
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Declarations
*
***/

enum {
    kExitBadArgs = 1,
    kExitConnectFailed = 2,
    kExitDisconnect = 3,
};


/****************************************************************************
*
*   MainShutdown
*
***/

class MainShutdown : public IDimAppShutdownNotify {
    void OnAppStartClientCleanup () override;
    bool OnAppQueryClientDestroy () override;
};

//===========================================================================
void MainShutdown::OnAppStartClientCleanup () {
}

//===========================================================================
bool MainShutdown::OnAppQueryClientDestroy () {
    return true;
}


/****************************************************************************
*
*   SocketConn
*
***/

class SocketConn : public IDimSocketNotify {
    void OnSocketConnect (const DimSocketConnectInfo & info) override;
    void OnSocketConnectFailed () override;
    void OnSocketRead (const DimSocketData & data) override;
    void OnSocketDisconnect () override;
};
static SocketConn s_conn;

//===========================================================================
void SocketConn::OnSocketConnect (const DimSocketConnectInfo & info) {
    cout << "Connected" << endl;
}

//===========================================================================
void SocketConn::OnSocketConnectFailed () {
    cout << "Connect failed" << endl;
    DimAppSignalShutdown(kExitConnectFailed);
}

//===========================================================================
void SocketConn::OnSocketRead (const DimSocketData & data) {
    cout.write(data.data, data.bytes);
    cout.flush();
}

//===========================================================================
void SocketConn::OnSocketDisconnect () {
    DimAppSignalShutdown(kExitDisconnect);
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
void Start (int argc, char * argv[]) {
    if (argc < 2) {
        cout << "tnet v1.0 (" __DATE__ ")\n"
            << "usage: tnet <remote address>\n";
        return DimAppSignalShutdown(kExitBadArgs);
    }

    SockAddr remoteAddr;
    Parse(&remoteAddr, argv[1]);
    SockAddr localAddr;
    cout << "Connecting to " << remoteAddr << " via " << localAddr << endl;
    DimSocketConnect(&s_conn, remoteAddr, localAddr);
}

//===========================================================================
int main(int argc, char * argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    MainShutdown cleanup;
    DimAppInitialize();
    DimAppMonitorShutdown(&cleanup);

    Start(argc, argv);    

    int code = DimAppWaitForShutdown();
    return code;
}
