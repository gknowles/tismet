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
*   SockAddr
*
***/

bool Parse (SockAddr * addr, const char src[]);
std::ostream & operator<< (std::ostream & os, const SockAddr & addr);
