// Copyright Glen Knowles 2015 - 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsm.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

const char kVersion[] = "1.0";


/****************************************************************************
*     
*   Application
*     
***/

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    cli.header("tismet v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);

    if (argc == 1) {
        auto os = logMsgInfo();
        return appSignalShutdown(cli.printHelp(os));
    }
    if (!cli.parse(argc, argv) || !cli.exec())
        return appSignalUsageError();
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

    int code = appRun(app, argc, argv);
    return code;
}
