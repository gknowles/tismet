// intern.h - dim core
#ifndef DIM_INTERN_INCLUDED
#define DIM_INTERN_INCLUDED

namespace Dim {


/****************************************************************************
*
*   File
*
***/

void iFileInitialize ();


/****************************************************************************
*
*   Socket
*
***/

void iSocketInitialize ();


/****************************************************************************
*
*   Task
*
***/

void iTaskInitialize ();
void iTaskDestroy ();


/****************************************************************************
*
*   Timer
*
***/

void iTimerInitialize ();
void iTimerDestroy ();


/****************************************************************************
*
*   Types
*
***/

int64_t iClockGetTicks ();

} // namespace

#endif
