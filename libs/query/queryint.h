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
    kFunc,
    kPath,
    kPathSeg,
    kSegLiteral,
    kSegBlot,
    kSegCharChoice,
    kSegStrChoice,
    kNum,
};

struct FuncNode : QueryInfo::Node {
    std::vector<Node> args;
};

struct PathNode : QueryInfo::Node {
    std::vector<std::unique_ptr<Node>> pathSegs;
};
struct PathSeg : QueryInfo::Node {
    std::vector<std::unique_ptr<Node>> segNodes;
};
struct SegLiteral : QueryInfo::Node {
    std::string_view val;
};
struct SegBlot : QueryInfo::Node {
};
struct SegCharChoice : QueryInfo::Node {
    std::bitset<256> vals;
};
struct SegStrChoice : QueryInfo::Node {
    std::vector<std::string_view> vals;
};

struct NumNode : QueryInfo::Node {
    double val;
};
