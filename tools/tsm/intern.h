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


/****************************************************************************
*
*   tcdump & tcload
*
***/

enum DumpFormat {
    kDumpFormatInvalid,
    kDumpFormat2018_1,
    kDumpFormat2018_2,
    kDumpFormats
};
char const * toString(DumpFormat type, char const def[] = nullptr);
DumpFormat fromString(std::string_view src, DumpFormat def);
