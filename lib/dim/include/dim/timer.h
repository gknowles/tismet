// timer.h - dim core
#ifndef DIM_TIMER_INCLUDED
#define DIM_TIMER_INCLUDED

#include "dim/config.h"

#include "dim/types.h"

#include <chrono>
#include <limits>
#include <memory>


/****************************************************************************
*
*   Constants
*
***/

const auto DIM_TIMER_INFINITE = Duration::max();


/****************************************************************************
*
*   Implemented by clients
*
***/

class IDimTimerNotify {
public:
    virtual ~IDimTimerNotify ();
    virtual Duration OnTimer () = 0;

private:
    friend class DimTimer;
    std::shared_ptr<DimTimer> m_timer;
};


/****************************************************************************
*
*   Public API
*
***/

void DimTimerUpdate (
    IDimTimerNotify * notify, 
    Duration wait, 
    bool onlyIfSooner = false
);
void DimTimerStopSync (IDimTimerNotify * notify);

#endif
