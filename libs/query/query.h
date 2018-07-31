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

namespace Query {

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
enum NodeType {
    kFunc,
    kNum,
    kString,
    kPath,

    // Internal node types
    kPathSeg,
    kSegEmpty,
    kSegLiteral,
    kSegBlot,
    kSegDoubleBlot,
    kSegCharChoice,
    kSegSegChoice,
};

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
struct Function {
    enum Type {
        kAlias,
        kAliasSub,
        kAverageSeries,
        kColor,
        kCountSeries,
        kDerivative,
        kDiffSeries,
        kDrawAsInfinite,
        kHighestCurrent,
        kHighestMax,
        kKeepLastValue,
        kLegendValue,
        kLineWidth,
        kMaximumAbove,
        kMaxSeries,
        kMinSeries,
        kMovingAverage,
        kMultiplySeries,
        kNonNegativeDerivative,
        kRemoveAboveValue,
        kRemoveBelowValue,
        kScale,
        kScaleToSeconds,
        kSumSeries,
        kStddevSeries,
        kTimeShift,

        kFuncTypes
    };

    Type type{kFuncTypes};
    std::vector<const Node *> args;
};

struct QueryInfo {
    char * text{nullptr};   // normalized query string
    Node * node{nullptr};
    PathType type{kExact};
    Dim::TempHeap heap;
};

// Returns false on malformed input, otherwise true and the query was
// successfully parsed.
bool parse(QueryInfo & qry, std::string_view src);

// Returns an entry for each segment of path. "out" will empty if query is
// not a path.
void getPathSegments(
    std::vector<PathSegment> * out,
    const QueryInfo & qry
);
// Use the node values returned by getPathSegments()
MatchResult matchSegment(const Node & node, std::string_view val);

NodeType getType(const Node & node);

// Returns a NAN if not a number node
double getNumber(const Node & node);

// empty string for non-string nodes
std::string_view getString(const Node & node);

// Returns false if not a function node
bool getFunc(Function * out, const Node & node);

const char * getFuncName(Query::Function::Type ftype, const char * defVal = "");

std::string toString(const Node & node);

} // namespace
