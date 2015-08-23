// intern.h - dim core
#ifndef DIM_INTERN_INCLUDED
#define DIM_INTERN_INCLUDED


/****************************************************************************
*
*   File
*
***/

void IDimFileInitialize ();


/****************************************************************************
*
*   Socket
*
***/

void IDimSocketInitialize ();


/****************************************************************************
*
*   Task
*
***/

void IDimTaskInitialize ();
void IDimTaskDestroy ();


/****************************************************************************
*
*   Timer
*
***/

void IDimTimerInitialize ();
void IDimTimerDestroy ();


/****************************************************************************
*
*   Types
*
***/

int64_t IDimClockGetTicks ();

#endif
