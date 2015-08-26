// main.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   EndpointFind
*
***/

class EndpointFind : public IDimEndpointNotify {
    void OnEndpointFound (Endpoint * ptr, int count) override;
};
static EndpointFind s_endFind;

//===========================================================================
void EndpointFind::OnEndpointFound (Endpoint * ptr, int count) {
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

class ListenSocket : public IDimSocketNotify {
    void OnSocketAccept (const DimSocketAcceptInfo & info) override;
    void OnSocketDisconnect () override;
    void OnSocketRead (const DimSocketData & data) override;
};

//===========================================================================
void ListenSocket::OnSocketAccept (const DimSocketAcceptInfo & info) {
    cout << "\n*** ACCEPTED " << info.remoteEnd << " to " 
        << info.localEnd << endl;
}

//===========================================================================
void ListenSocket::OnSocketDisconnect () {
    cout << "\n*** DISCONNECTED" << endl;
}

//===========================================================================
void ListenSocket::OnSocketRead (const DimSocketData & data) {
    cout.write(data.data, data.bytes);
    cout.flush();
}


/****************************************************************************
*
*   ListenNotify
*
***/

struct ListenNotify : IDimSocketListenNotify {
    void OnListenStop () override;
    unique_ptr<IDimSocketNotify> OnListenCreateSocket () override;
};
static ListenNotify s_listen;

//===========================================================================
void ListenNotify::OnListenStop () {
    cout << "*** STOPPED LISTENING" << endl;
}

//===========================================================================
unique_ptr<IDimSocketNotify> ListenNotify::OnListenCreateSocket () {
    return make_unique<ListenSocket>();
}


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
*   Start
*
***/

//===========================================================================
void Start (int argc, char * argv[]) {
    vector<Address> addrs;
    DimAddressGetLocal(&addrs);
    cout << "Local Addresses:" << endl;
    for (auto&& addr : addrs) {
        cout << addr << endl;
    }

    Endpoint end;
    Parse(&end, "127.0.0.1", 8888);
    DimSocketListen(&s_listen, end);
    //DimSocketStop(nullptr, Endpoint{});

    //if (argc > 1) {
    //    int cancelId;
    //    DimEndpointQuery(&cancelId, &s_endFind, argv[1], 0);
    //} else {
    //    DimAppSignalShutdown(8);
    //}
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
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
