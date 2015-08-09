// main.cpp - tismet
#include "pch.h"
#pragma hdrstop


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
*   main
*
***/

//===========================================================================
int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    int * a = new int[3];
    a[0] = 3;

    int code = 0;
    int limit = argc > 1 ? atoi(argv[1]) : 1;
    for (int i = 1; i < limit + 1; ++i) {
        if (limit > 1)
            DimErrorLog{kInfo} << "Run #" << i;
        MainShutdown cleanup;
        DimAppInitialize();
        DimAppMonitorShutdown(&cleanup);
        DimAppSignalShutdown(9);
        code = DimAppWaitForShutdown();
    }
    return code;
}
