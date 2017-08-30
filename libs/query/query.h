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
        fWild = 1,   // query has paths with wildcards
    };

    enum NodeType : int8_t;
    struct Node;
    struct PathSegment {
        std::string_view prefix;
        QueryFlags flags{};
        const Node * node{nullptr};
    };

    char * text{nullptr};   // normalized query string
    Node * node{nullptr};
    QueryFlags flags{};
    Dim::TempHeap heap;
};

// Returns false on malformed input, otherwise true and the query was 
// successfully parsed.
bool queryParse(QueryInfo & qry, std::string_view src);

// Returns returns an entry for each segment of path. "out" will empty if 
// query is not a path.
void queryPathSegments(
    std::vector<QueryInfo::PathSegment> & out, 
    const QueryInfo & qry
);
// Use the node values returned by queryPathSegments()
bool queryMatchSegment(
    const QueryInfo::Node * node,
    std::string_view val
);
