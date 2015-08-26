// types.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   Endpoint
*
***/

bool Parse (Endpoint * out, const char src[]);
std::ostream & operator<< (std::ostream & os, const Endpoint & src);


/****************************************************************************
*
*   DimClock
*
***/

const int64_t kDimClockTicksPerTimeT{10'000'000};

//===========================================================================
// static
DimClock::time_point DimClock::now() noexcept {
    return (time_point(duration(IDimClockGetTicks())));
}

//===========================================================================
// static 
time_t DimClock::to_time_t(const time_point& time) noexcept {
    return ((time_t)(time.time_since_epoch().count()
        / kDimClockTicksPerTimeT));
}

//===========================================================================
// static 
DimClock::time_point DimClock::from_time_t(time_t tm) noexcept {
    return (time_point(duration(tm * kDimClockTicksPerTimeT)));
}
