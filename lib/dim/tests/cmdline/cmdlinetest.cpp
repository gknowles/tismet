// cmdlinetest.cpp - dim test cmdline
#include "pch.h"
#pragma hdrstop

using namespace Dim::CmdLine;

int main(int argc, char *argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF 
        | _CRTDBG_LEAK_CHECK_DF
    );
    _set_error_mode(_OUT_TO_MSGBOX);

    Option<int> num{"-n --number", "it's an important number", 1};
    ParseOptions(argc, argv);

    return 0;
}
