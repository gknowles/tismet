// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// queryint.h - tismet query
//
// Implementation of graphite's carbon protocol for receiving metric data
#pragma once

#include <bitset>
#include <memory>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   Query::QueryInfo
*
***/

struct Query::Node : Dim::ListBaseLink<> {
    NodeType type;
};

Query::Node * addPath(Query::QueryInfo * qi);
void endPath(Query::QueryInfo * qi, Query::Node * node);
Query::Node * addSeg(Query::QueryInfo * qi, Query::Node * path);
void endSeg(Query::QueryInfo * qi, Query::Node * node);
Query::Node * addSegLiteral(
    Query::QueryInfo * qi,
    Query::Node * seg,
    std::string_view val
);
Query::Node * addSegBlot(Query::QueryInfo * qi, Query::Node * seg);
Query::Node * addSegChoices(
    Query::QueryInfo * qi,
    Query::Node * seg,
    std::bitset<256> & vals
);
Query::Node * addSegStrChoices(Query::QueryInfo * qi, Query::Node * seg);
Query::Node * addSegChoice(
    Query::QueryInfo * qi,
    Query::Node * seg,
    std::string_view val
);
Query::Node * addFunc(Query::QueryInfo * qi, Query::Function::Type type);
Query::Node * addFuncArg(
    Query::QueryInfo * qi,
    Query::Node * func,
    Query::Function::Type type
);
Query::Node * addPathArg(Query::QueryInfo * qi, Query::Node * func);
Query::Node * addNumArg(
    Query::QueryInfo * qi,
    Query::Node * func,
    double val
);
Query::Node * addStringArg(
    Query::QueryInfo * qi,
    Query::Node * func,
    std::string_view val
);
