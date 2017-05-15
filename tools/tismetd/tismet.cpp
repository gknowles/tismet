// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tismet.cpp - tismetd
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
    void onEndpointFound (const Endpoint * ptr, int count) override;
};
static EndpointFind s_endFind;

//===========================================================================
void EndpointFind::onEndpointFound (const Endpoint * ptr, int count) {
    cout << "\nDNS Addresses:" << endl;
    for (int i = 0; i < count; ++i) {
        cout << ptr[i] << endl;
    }
}


/****************************************************************************
*
*   MainShutdown
*
***/

class MainShutdown : public IShutdownNotify {
    void onShutdownClient (bool firstTry) override;
};
static MainShutdown s_cleanup;

//===========================================================================
void MainShutdown::onShutdownClient (bool firstTry) {
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
    shutdownMonitor(&s_cleanup);

    vector<Address> addrs;
    addressGetLocal(&addrs);
    cout << "Local Addresses:" << endl;
    for (auto&& addr : addrs) {
        cout << addr << endl;
    }
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
