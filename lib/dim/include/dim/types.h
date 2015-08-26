// types.h - dim core
#ifndef DIM_TYPES_INCLUDED
#define DIM_TYPES_INCLUDED

#include "dim/config.h"

#include <chrono>
#include <limits>
#include <memory>
#include <ratio>

//using namespace std::literals;


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

    static time_point now() noexcept;

// C conversions
    static time_t to_time_t(const time_point& time) noexcept;
    static time_point from_time_t(time_t tm) noexcept;
};

typedef DimClock::duration Duration;
typedef DimClock::time_point TimePoint;


/****************************************************************************
*
*   Networking
*
***/

// IP v4 or v6 address
struct Address {
    int32_t data[4]{};

    bool operator== (const Address & right) const;
    explicit operator bool () const;
};
struct Endpoint {
    Address addr;
    unsigned port{0};

    bool operator== (const Endpoint & right) const;
    explicit operator bool () const;
};
struct Network {
    Address addr;
    int mask{0};
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
