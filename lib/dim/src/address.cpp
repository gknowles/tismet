// address.cpp - dim services
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
*   NetAddr
*
***/

//===========================================================================
bool NetAddr::operator== (const NetAddr & right) const {
    return memcmp(this, &right, sizeof *this) == 0;
}

//===========================================================================
NetAddr::operator bool () const {
    return data[3] || data[0] || data[1] || data[2];
}

/****************************************************************************
*
*   SockAddr
*
***/

//===========================================================================
bool SockAddr::operator== (const SockAddr & right) const {
    return port == right.port && addr == right.addr;
}

//===========================================================================
SockAddr::operator bool () const {
    return port || addr;
}

bool Parse (SockAddr * addr, const char src[]);
std::ostream & operator<< (std::ostream & os, const SockAddr & addr);
