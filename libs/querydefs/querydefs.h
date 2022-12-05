// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// querydefs.h - tismet querydefs
#pragma once

#include "cppconf/cppconf.h"

#include "core/core.h"

#include <bitset>
#include <memory>
#include <string_view>
#include <vector>

// forward declarations
namespace Eval::Function {
    enum Type : int;
}

namespace Query {


/****************************************************************************
*
*   Declarations
*
***/

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

struct Node : Dim::ListBaseLink<> {
    NodeType type;
};

struct PathSegment {
    union {
        // for kExact and kCondition, prefix enforced by condition
        std::string_view prefix;

        // for kDynamicAny, segments spanned in current permutation
        unsigned count;
    };
    PathType type{kExact};
    const Node * node{};

    PathSegment() { prefix = {}; }
};
struct Function {
    Eval::Function::Type type{};
    std::vector<const Node *> args;
};

struct QueryInfo {
    char * text{}; // normalized query string
    Node * node{};
    PathType type{kExact};
    Dim::TempHeap heap;
};


/****************************************************************************
*
*   Querying abstract syntax tree
*
***/

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
double asNumber(const Node & node);

// empty string for non-string nodes
std::string_view asString(const Node & node);
std::shared_ptr<char[]> asSharedString(const Node & node);

// Returns false if not a function node
bool getFunc(Function * out, const Node & node);

struct ITokenConvNotify {
    virtual ~ITokenConvNotify() = default;
    virtual const Dim::TokenTable & funcTypeTbl() const = 0;
};
std::string toString(
    const Node & node,
    const ITokenConvNotify * notify
);


/****************************************************************************
*
*   Abstract syntax tree builders
*
***/

Query::Node * addPath(Query::QueryInfo * qi);
void endPath(Query::QueryInfo * qi, Query::Node * node);
Query::Node * addSeg(Query::QueryInfo * qi, Query::Node * path);
void endSeg(Query::QueryInfo * qi, Query::Node * node, Query::Node * parent);
Query::Node * addSegEmpty(Query::QueryInfo * qi, Query::Node * seg);
Query::Node * addSegLiteral(
    Query::QueryInfo * qi,
    Query::Node * seg,
    std::string_view val
);
Query::Node * addSegBlot(Query::QueryInfo * qi, Query::Node * seg);
Query::Node * addSegCharChoices(
    Query::QueryInfo * qi,
    Query::Node * seg,
    std::bitset<256> & vals
);
Query::Node * addSegSegChoices(Query::QueryInfo * qi, Query::Node * seg);
Query::Node * addFunc(Query::QueryInfo * qi, Eval::Function::Type type);
Query::Node * addFuncArg(
    Query::QueryInfo * qi,
    Query::Node * func,
    Eval::Function::Type type
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

} // namespace
