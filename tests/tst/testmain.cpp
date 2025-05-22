// Copyright Glen Knowles 2018 - 2025.
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
        .action([&](Cli & cli) { run(); });

    tests().link(this);
}

//===========================================================================
void ITest::run() {
    cout << name() << "..." << endl;
    onTestRun();
    appSignalShutdown();
    assert(appMode() == kRunStopping);
}


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static void initApp(Cli & cli) {
    funcInitialize();
}

//===========================================================================
static void allCmd(Cli & cli) {
    vector<ITest *> all;
    for (auto&& test : tests()) {
        all.push_back(&test);
    }
    sort(
        all.begin(),
        all.end(),
        [](auto & a, auto & b) { return a->name() < b->name(); }
    );
    for (auto && test : all) {
        test->run();
    }
    cout << endl;
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

    Cli cli;
    cli.helpCmd().helpNoArgs();
    cli.beforeExec(initApp);
    cli.command("all")
        .desc("Run all tests.")
        .action(allCmd);
    int code = appRun(argc, argv, kVersion);
    if (!logGetMsgCount(kLogTypeError))
        cout << "\nAll tests passed\n";
    return code;
}
