// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// eval.h - tismet eval
#pragma once

#include "core/core.h"

#include "db/db.h"


/****************************************************************************
*
*   Evaluate queries
*
***/

void evalInitialize(DbHandle db);

struct IEvalNotify : IDbEnumNotify {
    // Called after the last series for the query has ended.
    virtual void onEvalEnd() {}
    virtual void onEvalError(std::string_view errmsg) = 0;
};
void evalAdd(
    IEvalNotify * notify,
    const std::vector<std::string_view> & targets,
    Dim::TimePoint from,
    Dim::TimePoint until,
    size_t maxPoints = 0    // 0 for no maximum
);
