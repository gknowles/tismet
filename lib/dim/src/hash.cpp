// hash.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Variables
*
***/


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iHashInitialize () {
    if (sodium_init() == -1)
        logMsgCrash() << "sodium_init: failed";
}

} // namespace
