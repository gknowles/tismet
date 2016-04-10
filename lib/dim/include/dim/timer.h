// timer.h - dim core
#pragma once

#include "dim/config.h"
#include "dim/types.h"

#include <memory>

namespace Dim {

/****************************************************************************
*
*   Constants
*
***/

const auto kTimerInfinite = Duration::max();


/****************************************************************************
*
*   Implemented by clients
*
***/

class ITimerNotify {
public:
    virtual ~ITimerNotify ();
    virtual Duration onTimer (TimePoint now) = 0;

private:
    friend class Timer;
    std::shared_ptr<Timer> m_timer;
};


/****************************************************************************
*
*   Public API
*
***/

void timerUpdate (
    ITimerNotify * notify, 
    Duration wait, 
    bool onlyIfSooner = false
);
void timerStopSync (ITimerNotify * notify);

} // namespace
