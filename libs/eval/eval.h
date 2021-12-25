// Copyright Glen Knowles 2018 - 2021.
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

struct IEvalNotify : IDbDataNotify {
    // Called after the last series for the query has ended.
    virtual void onEvalEnd() {}
    virtual void onEvalError(std::string_view errmsg) = 0;
};
void evaluate(
    IEvalNotify * notify,
    std::string_view target,
    Dim::TimePoint from,
    Dim::TimePoint until,
    size_t maxPoints = 0    // 0 for no maximum
);


/****************************************************************************
*
*   Parse queries
*
***/

namespace Query {
    struct Node;
    struct QueryInfo;
};

std::string toString(const Query::Node & node);

bool parse(Query::QueryInfo & qry, std::string_view src);
