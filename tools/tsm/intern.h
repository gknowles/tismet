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
    DbProgressInfo const * limit = {},
    std::chrono::duration<double> timeLimit = {}
);
void tcLogShutdown(DbProgressInfo const * total = {});
