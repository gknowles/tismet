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
    DimAppSignalShutdown(9);
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

    if (argc > 1) {
        int cancelId;
        DimAddressQuery(&cancelId, &s_addrFind, argv[1], 0);
    } else {
        DimAppSignalShutdown(8);
    }
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
