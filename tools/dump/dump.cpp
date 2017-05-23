// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// load.cpp - tismet dump
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
namespace fs = std::experimental::filesystem;


/****************************************************************************
*
*   Declarations
*
***/

const char kVersion[] = "1.0";


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
class Application : public IAppNotify {
    void onAppRun() override;
};
} // namespace

//===========================================================================
void Application::onAppRun () {
    Cli cli;
    cli.header("dump v"s + kVersion + " (" __DATE__ ")");
    cli.versionOpt(kVersion);
    auto & dat = cli.opt<string>("[dat file]");
    if (!cli.parse(m_argc, m_argv))
        return appSignalUsageError();
    if (!dat)
        return appSignalUsageError("No value given for <dat file[.dat]>");
        
    cout << "Dumping " << *dat << endl;

    auto h = tsdOpen(*dat);
    tsdDump(cout, h);
    tsdClose(h);

    appSignalShutdown(EX_OK);
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
        | _CRTDBG_DELAY_FREE_MEM_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    Application app;
    int code = appRun(app, argc, argv);
    return code;
}
