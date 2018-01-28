// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// query.h - tismet query
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

namespace QueryFunc {
    enum Type {
        kAlias,
        kDerivative,
        kKeepLastValue,
        kMaximumAbove,
        kNonNegativeDerivative,
        kScale,
        kSum,
        kTimeShift,

        kFuncTypes
    };
}

struct QueryInfo {
    enum PathType {
        // for both query infos and path segments
        kExact,         // literal
        kCondition,     // char choice, string choice, or embedded blot
        kAny,           // can be any value

        // only for path segments
        kDynamicAny,   // matches zero or more segments of any value
    };
    enum MatchResult {
        kNoMatch = 0,
        kMatch = 1,

        // matches this segment and also any number of following segments
        kMatchRest = 2,
    };

    enum NodeType : int8_t;
    struct Node;
    struct PathSegment {
        union {
            // for kExact and kCondition, prefix enforced by condition
            std::string_view prefix;

            // for kDynamicAny, segments spanned in current permutation
            unsigned count;
        };
        PathType type{kExact};
        const Node * node{nullptr};

        PathSegment() { prefix = {}; }
    };

    char * text{nullptr};   // normalized query string
    Node * node{nullptr};
    PathType type{kExact};
    Dim::TempHeap heap;
};

// Returns false on malformed input, otherwise true and the query was
// successfully parsed.
bool queryParse(QueryInfo & qry, std::string_view src);

// Returns an entry for each segment of path. "out" will empty if query is
// not a path.
void queryPathSegments(
    std::vector<QueryInfo::PathSegment> & out,
    const QueryInfo & qry
);
// Use the node values returned by queryPathSegments()
QueryInfo::MatchResult queryMatchSegment(
    const QueryInfo::Node * node,
    std::string_view val
);
