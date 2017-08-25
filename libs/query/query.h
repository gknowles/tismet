// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// query.h - tismet query
//
// Implemetation of graphite's carbon protocol for receiving metric data
#pragma once

#include "cppconf/cppconf.h"
#include "core/core.h"

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
    enum QueryFlags : uint8_t {
        fWild = 1,
    };

    enum NodeType : int8_t;

    struct Node : Dim::ListBaseLink<Node> {
        NodeType type;
    };

    char * text{nullptr};
    Node * node{nullptr};
    QueryFlags flags{};
    Dim::TempHeap heap;
};

// Returns false on malformed input, and true otherwise (query successfully
// parsed).
bool queryParse(QueryInfo & qry, std::string_view src);

void queryNormalize(QueryInfo & qry);
