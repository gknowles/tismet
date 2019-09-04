// Copyright Glen Knowles 2018 - 2019.
// Distributed under the Boost Software License, Version 1.0.
//
// testmain.cpp - tismet test
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


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
    : m_name{name}
{
    Cli cli;
    cli.command(string(name))
        .desc(string(desc))
        .action([&](Cli & cli) { this->onTestRun(); return true; });

    tests().link(this);
}

//===========================================================================
void ITest::onTestDefine(Cli & cli)
{}


/****************************************************************************
*
*   Application
*
***/

//===========================================================================
static bool allCmd(Cli & cli) {
    for (auto && test : tests()) {
        cout << test.name() << "...\n";
        test.onTestRun();
    }
    cout << endl;
    return true;
}

//===========================================================================
static void app(int argc, char * argv[]) {
    Cli cli;
    auto version = string("(" __DATE__ ")");
    cli.header("tst "s + version);
    cli.versionOpt(version, "tst");
    cli.helpCmd().helpNoArgs();
    cli.command("all")
        .desc("Run all tests.")
        .action(allCmd);
    for (auto && test : tests()) {
        cli.command(string(test.name()));
        test.onTestDefine(cli);
    }
    if (!cli.exec(argc, argv))
        return appSignalUsageError();
    if (cli.commandMatched() == "help")
        return appSignalShutdown(EX_OK);

    if (int errors = logGetMsgCount(kLogTypeError)) {
        ConsoleScopedAttr attr(kConsoleError);
        cerr << "*** " << errors << " FAILURES" << endl;
        appSignalShutdown(EX_SOFTWARE);
    } else {
        cout << "All tests passed" << endl;
        appSignalShutdown(EX_OK);
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
        | _CRTDBG_DELAY_FREE_MEM_DF
        //| _CRTDBG_CHECK_ALWAYS_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    funcInitialize();
    int code = appRun(app, argc, argv);
    return code;
}
