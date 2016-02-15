// wintypes.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Time
*
***/

//===========================================================================
int64_t iClockGetTicks () {
    LARGE_INTEGER out;
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    out.HighPart = ft.dwHighDateTime;
    out.LowPart = ft.dwLowDateTime;
    return out.QuadPart;
}

} // namespace
