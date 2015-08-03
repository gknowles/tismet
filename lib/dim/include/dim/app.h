// app.h - dim core
#ifndef DIM_APP_INCLUDED
#define DIM_APP_INCLUDED

#include "dim/config.h"

// forward declarations
enum RunMode;


class IDimAppShutdownNotify {
public:
    virtual ~IDimAppShutdownNotify () {}

    virtual void OnAppStartClientCleanup () {}
    virtual bool OnAppQueryClientDestroy () { return true; }
    virtual void OnAppStartServerCleanup () {}
    virtual bool OnAppQueryServerDestroy () { return true; }
    virtual void OnAppStartConsoleCleanup () {}
    virtual bool OnAppQueryConsoleDestroy () { return true; }
};

void DimAppInitialize ();
void DimAppSignalShutdown (int exitcode = 0);

// returns exit code
int DimAppWaitForShutdown ();

void DimAppMonitorShutdown (IDimAppShutdownNotify * cleanup);
bool DimQueryDestroyFailed ();

RunMode DimAppMode ();

#endif
