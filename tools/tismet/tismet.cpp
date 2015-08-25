// main.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   AddrFind
*
***/

class AddrFind : public IDimAddressNotify {
    void OnAddressFound (SockAddr * addr, int count) override;
};
static AddrFind s_addrFind;

//===========================================================================
void AddrFind::OnAddressFound (SockAddr * addr, int count) {
    cout << "\nDNS Addresses:" << endl;
    for (int i = 0; i < count; ++i) {
        cout << addr[i] << endl;
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
    void OnSocketRead (const DimSocketData & data) override;
};

//===========================================================================
void ListenSocket::OnSocketAccept (const DimSocketAcceptInfo & info) {
    cout << "*** ACCEPTED" << endl;
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
    vector<NetAddr> addrs;
    DimAddressGetLocal(&addrs);
    cout << "Local Addresses:" << endl;
    for (auto&& addr : addrs) {
        cout << addr << endl;
    }

    SockAddr addr;
    Parse(&addr, "127.0.0.1", 8888);
    DimSocketListen(&s_listen, addr);
    //DimSocketStop(nullptr, SockAddr{});

    //if (argc > 1) {
    //    int cancelId;
    //    DimAddressQuery(&cancelId, &s_addrFind, argv[1], 0);
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
