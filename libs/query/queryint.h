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
    kSegCharChoice,
    kSegStrChoice,
    kNum,

    kBeforeFirstFunc,
    kFnMaximumAbove,
    kFnSum,
    kAfterLastFunc,
};

QueryInfo::Node * addPath(QueryInfo * qi);
QueryInfo::Node * addSeg(QueryInfo::Node * path);
QueryInfo::Node * addSegLiteral(QueryInfo::Node * seg, std::string_view val);
QueryInfo::Node * addSegBlot(QueryInfo::Node * seg);
QueryInfo::Node * addSegChoice(
    QueryInfo::Node * seg, 
    std::bitset<256> && vals
);
QueryInfo::Node * addSegChoice(
    QueryInfo::Node * seg,
    std::vector<std::string_view> && vals
);
QueryInfo::Node * addFunc(QueryInfo * qi, QueryInfo::NodeType type);
QueryInfo::Node * addFuncArg(QueryInfo::Node * func, QueryInfo::NodeType type);
QueryInfo::Node * addPathArg(QueryInfo::Node * func);
QueryInfo::Node * addNumArg(QueryInfo::Node * func, double val);
