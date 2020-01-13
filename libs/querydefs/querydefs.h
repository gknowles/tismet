// Copyright Glen Knowles 2017 - 2018.
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

struct Node : Dim::ListLink<> {
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
    Node const * node{};

    PathSegment() { prefix = {}; }
};
struct Function {
    Eval::Function::Type type{};
    std::vector<Node const *> args;
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
    QueryInfo const & qry
);
// Use the node values returned by getPathSegments()
MatchResult matchSegment(Node const & node, std::string_view val);

NodeType getType(Node const & node);

// Returns a NAN if not a number node
double asNumber(Node const & node);

// empty string for non-string nodes
std::string_view asString(Node const & node);
std::shared_ptr<char[]> asSharedString(Node const & node);

// Returns false if not a function node
bool getFunc(Function * out, Node const & node);

struct ITokenConvNotify {
    virtual ~ITokenConvNotify() = default;
    virtual Dim::TokenTable const & funcTypeTbl() const = 0;
};
std::string toString(
    Node const & node,
    ITokenConvNotify const * notify
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
