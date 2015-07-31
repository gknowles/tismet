// main.cpp - tismet
#include "dim/app.h"
#include "dim/app.h"

#include <crtdbg.h>
#include <cstdio>
#include <cstdlib>

class TestCleanup : public IAppCleanup {
    void OnAppStartClientCleanup () override;
    bool OnAppQueryClientDestroy () override;
};

void TestCleanup::OnAppStartClientCleanup () {
}

bool TestCleanup::OnAppQueryClientDestroy () {
    return true;
}

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
            printf("Run #%i\n", i);
        TestCleanup cleanup;
        Initialize();
        RegisterCleanup(&cleanup);
        SignalShutdown(9);
        code = WaitForShutdown();
    }
    return code;
}
