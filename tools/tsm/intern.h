// Copyright Glen Knowles 2018 - 2026.
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
    Dim::Duration timeLimit = {}
);
void tcLogShutdown(
    const DbProgressInfo * total = {},
    std::string_view text = "Done",
    Dim::LogType logType = Dim::LogType::kLogTypeInfo
);
