// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// queryint.h - tismet query
//
// Implemetation of graphite's carbon protocol for receiving metric data
#pragma once

#include <bitset>
#include <memory>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   QueryInfo
*
***/

enum QueryInfo::NodeType : int8_t {
    kPath,
    kPathSeg,
    kSegLiteral,
    kSegBlot,
    kSegDoubleBlot,
    kSegCharChoice,
    kSegStrChoice,

    kFunc,
    kNum,
    kString,
};

struct QueryInfo::Node : Dim::ListBaseLink<> {
    NodeType type;
};

QueryInfo::Node * addPath(QueryInfo * qi);
void endPath(QueryInfo * qi, QueryInfo::Node * node);
QueryInfo::Node * addSeg(QueryInfo * qi, QueryInfo::Node * path);
void endSeg(QueryInfo * qi, QueryInfo::Node * node);
QueryInfo::Node * addSegLiteral(
    QueryInfo * qi,
    QueryInfo::Node * seg,
    std::string_view val
);
QueryInfo::Node * addSegBlot(QueryInfo * qi, QueryInfo::Node * seg);
QueryInfo::Node * addSegChoices(
    QueryInfo * qi,
    QueryInfo::Node * seg,
    std::bitset<256> & vals
);
QueryInfo::Node * addSegStrChoices(QueryInfo * qi, QueryInfo::Node * seg);
QueryInfo::Node * addSegChoice(
    QueryInfo * qi,
    QueryInfo::Node * seg,
    std::string_view val
);
QueryInfo::Node * addFunc(QueryInfo * qi, QueryFunc::Type type);
QueryInfo::Node * addFuncArg(
    QueryInfo * qi,
    QueryInfo::Node * func,
    QueryFunc::Type type
);
QueryInfo::Node * addPathArg(QueryInfo * qi, QueryInfo::Node * func);
QueryInfo::Node * addNumArg(
    QueryInfo * qi,
    QueryInfo::Node * func,
    double val
);
QueryInfo::Node * addStringArg(
    QueryInfo * qi,
    QueryInfo::Node * func,
    std::string_view val
);
