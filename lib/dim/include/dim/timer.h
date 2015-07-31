// timer.h - dim core
#ifndef DIM_TIMER_INCLUDED
#define DIM_TIMER_INCLUDED

#include "dim/config.h"

#include "dim/types.h"

#include <chrono>
#include <limits>
#include <memory>

const auto TIMER_WAIT_FOREVER = Duration::max();

class ITimerNotify {
public:
    virtual ~ITimerNotify ();
    virtual Duration OnTimer () = 0;

private:
    friend class Timer;
    std::shared_ptr<Timer> m_timer;
};

void TimerUpdate (
    ITimerNotify * notify, 
    Duration wait, 
    bool onlyIfSooner = false
);
void TimerStopSync (ITimerNotify * notify);

#endif
