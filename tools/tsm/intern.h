// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tsm


/****************************************************************************
*
*   tcmain
*
***/

void tcLogStart(
    const DbProgressInfo * limit = {},
    std::chrono::duration<double> timeLimit = {}
);
void tcLogShutdown(const DbProgressInfo * total = {});
