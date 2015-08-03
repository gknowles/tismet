// types.h - dim core
#ifndef DIM_TYPES_INCLUDED
#define DIM_TYPES_INCLUDED

#include "dim/config.h"

#include <chrono>
#include <limits>
#include <memory>
#include <ratio>


/****************************************************************************
*
*   DimClock
*
***/

struct DimClock {
    typedef int64_t rep;
    typedef std::ratio_multiply<std::ratio<100, 1>, std::nano> period;
    typedef std::chrono::duration<rep, period> duration;
    typedef std::chrono::time_point<DimClock> time_point;
    static const bool is_monotonic = false;
    static const bool is_steady = false;

    static time_point now() _NOEXCEPT
    {	// get current time
        return (time_point(duration(_Xtime_get_ticks())));
    }

// C conversions
    static time_t to_time_t(const time_point& _Time) _NOEXCEPT
    {	// convert to time_t
        return ((time_t)(_Time.time_since_epoch().count()
            / _XTIME_TICKS_PER_TIME_T));
    }

    static time_point from_time_t(time_t _Tm) _NOEXCEPT
	{	// convert from time_t
	    return (time_point(duration(_Tm * _XTIME_TICKS_PER_TIME_T)));
    }
};

typedef DimClock::duration Duration;
typedef DimClock::time_point TimePoint;


/****************************************************************************
*
*   Networking
*
***/

// IP v4 or v6 address
struct NetAddr {
    int8_t data[16];
};
struct SockAddr {
    NetAddr addr;
    unsigned port;
};


/****************************************************************************
*
*   Run modes
*
***/

enum RunMode {
    kRunStopped,
    kRunStarting,
    kRunRunning,
    kRunStopping,
};


#endif
