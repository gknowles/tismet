// app.h - dim core
#ifndef DIM_APP_INCLUDED
#define DIM_APP_INCLUDED

#include "dim/config.h"

// forward declarations
enum ERunMode;


class IAppCleanup {
public:
    virtual ~IAppCleanup () {}

    virtual void OnAppStartClientCleanup () {}
    virtual bool OnAppQueryClientDestroy () { return true; }
    virtual void OnAppStartServerCleanup () {}
    virtual bool OnAppQueryServerDestroy () { return true; }
    virtual void OnAppStartConsoleCleanup () {}
    virtual bool OnAppQueryConsoleDestroy () { return true; }
};

void Initialize ();
void SignalShutdown (int exitcode = 0);

// returns exit code
int WaitForShutdown ();

void RegisterCleanup (IAppCleanup * cleanup);
bool QueryDestroyFailed ();

ERunMode AppMode ();

#endif
