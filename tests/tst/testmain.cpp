// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// testmain.cpp - tismet test
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const VersionInfo kVersion = { 1, 1 };


/****************************************************************************
*
*   ITest
*
***/

//===========================================================================
static List<ITest> & tests() {
    static List<ITest> s_tests;
    return s_tests;
}

//===========================================================================
ITest::ITest (std::string_view name, std::string_view desc) 
    : m_name(name)
{
    m_cli.command(m_name)
        .desc(string(desc))
        .action([&](Cli & cli) { 
            cout << this->name() << "...\n";
            this->onTestRun(); 
        });

    tests().link(this);
}


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static void allCmd(Cli & cli) {
    for (auto && test : tests()) {
        cout << test.name() << "...\n";
        test.onTestRun();
    }
    cout << endl;
}

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    cli.helpCmd().helpNoArgs();
    cli.command("all")
        .desc("Run all tests.")
        .action(allCmd);
    if (!cli.exec(argc, argv))
        return appSignalUsageError();
    testSignalShutdown();
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
        //| _CRTDBG_CHECK_ALWAYS_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    funcInitialize();
    int code = appRun(app, argc, argv, kVersion);
    return code;
}
