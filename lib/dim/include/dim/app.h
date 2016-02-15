// app.h - dim core
#ifndef DIM_APP_INCLUDED
#define DIM_APP_INCLUDED

#include "dim/config.h"

namespace Dim {

// forward declarations
enum RunMode;


class IAppShutdownNotify {
public:
    virtual ~IAppShutdownNotify () {}

    virtual void onAppStartClientCleanup () {}
    virtual bool onAppQueryClientDestroy () { return true; }
    virtual void onAppStartServerCleanup () {}
    virtual bool onAppQueryServerDestroy () { return true; }
    virtual void onAppStartConsoleCleanup () {}
    virtual bool onAppQueryConsoleDestroy () { return true; }
};

void appInitialize ();
void appSignalShutdown (int exitcode = 0);

// returns exit code
int appWaitForShutdown ();

void appMonitorShutdown (IAppShutdownNotify * cleanup);
bool appQueryDestroyFailed ();

RunMode appMode ();

} // namespace

#endif
