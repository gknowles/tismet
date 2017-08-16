// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// query.h - tismet query
//
// Implemetation of graphite's carbon protocol for receiving metric data
#pragma once

#include "cppconf/cppconf.h"

#include <bitset>
#include <memory>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   Public API
*
***/

struct QueryInfo {
    enum NodeType : int8_t;
    struct Node {
        NodeType type;
    };

    std::string text;
    std::unique_ptr<Node> node;
};

// Returns false on malformed input, and true otherwise (query successfully
// parsed).
bool queryParse(QueryInfo & qry, std::string_view src);
