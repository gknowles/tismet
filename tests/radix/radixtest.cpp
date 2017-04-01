// radixtest.cpp - tismet test radix
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

#define EXPECT(...)                                                         \
    if (!bool(__VA_ARGS__)) {                                               \
        logMsgError() << "Line " << (line ? line : __LINE__) << ": EXPECT(" \
                      << #__VA_ARGS__ << ") failed";                        \
    }


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void tests() {
    int line = 0;
    int digits[10];

    RadixDigits rd{100, 4095};
    auto count = rd.convert(digits, size(digits), 4032);
    EXPECT(count == 3);
    EXPECT(vector<int>(digits, digits + 3) == vector<int>({6, 11, 7}));

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
*   Application
*
***/

namespace {

class Application : public IAppNotify {
    // IAppNotify
    void onAppRun() override;
};

} // namespace

//===========================================================================
void Application::onAppRun() {
    Cli cli;
    auto & blkSize = cli.opt<size_t>("b", 4032)
        .desc("size of blocks used by radix index");
    auto & maxPages = cli.opt<size_t>(
        "m", 
        (size_t) numeric_limits<uint32_t>::max() + 1)
        .desc("maximum number of pages allowed in index");
    auto & vals = cli.optVec<uint32_t>("[value]")
        .desc("values to translate");
    auto & test = cli.opt<bool>("test")
        .desc("run internal unit tests");
    if (!cli.parse(m_argc, m_argv))
        return appSignalUsageError();

    if (*test)
        return tests();

    RadixDigits rd{*blkSize, *maxPages - 1};
    cout << rd << endl;

    int digits[10];
    for (auto && val : *vals) {
        rd.convert(digits, size(digits), val);
        cout << val << ":";
        for (auto && d : digits) {
            if (d == -1)
                break;
            cout << ' ' << d;
        }
        cout << endl;
    }
    appSignalShutdown(EX_OK);
}


/****************************************************************************
*
*   External
*
***/

//===========================================================================
int main(int argc, char * argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);
    Application app;
    return appRun(app, argc, argv);
}
