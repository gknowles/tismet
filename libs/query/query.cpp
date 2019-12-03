// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// query.cpp - tismet query
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
using namespace Query;

/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kQueryMaxSize = 8192;


/****************************************************************************
*
*   Node declarations
*
***/

namespace {

struct FuncNode : Node {
    Eval::Function::Type func;
    List<Node> args;
};

struct PathNode : Node {
    List<Node> segs;
};
struct PathSeg : Node {
    List<Node> nodes;
};
struct SegEmpty : Node {
};
struct SegLiteral : Node {
    string_view val;
};
struct SegBlot : Node {
    int count{0};
};
struct SegCharChoice : Node {
    static_assert(sizeof(unsigned long long) == sizeof(uint64_t));
    bitset<256> vals;
};
struct SegSegChoice : Node {
    List<Node> segs;
};

struct NumNode : Node {
    double val;
};
struct StringNode : Node {
    string_view val;
};

} // namespace


/****************************************************************************
*
*   Helpers
*
***/

/****************************************************************************
*
*   QueryParserBase
*
***/

//===========================================================================
bool QueryParserBase::startFunc (Eval::Function::Type type) {
    auto func = m_nodes.empty()
        ? addFunc(m_query, type)
        : addFuncArg(m_query, m_nodes.back(), type);
    m_nodes.push_back(func);
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/



/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
bool Query::parse(
    QueryInfo & qry,
    string_view src,
    const ITokenConvNotify * notify
) {
    assert(*src.end() == 0);
    qry = {};
    auto ptr = src.data();
    auto parser = QueryParser{&qry};
    if (!parser.parse(ptr)) {
        logParseError("Invalid query", "", parser.errpos(), src);
        return false;
    }
    assert(qry.node);

    // normalize
    string text = toString(*qry.node, notify);
    qry = {};
    qry.text = qry.heap.strdup(text.c_str());
    parser = QueryParser{&qry};
    bool success [[maybe_unused]] = parser.parse(qry.text);
    assert(success);

    // check if query is kAny
    if (qry.node->type == kPath) {
        auto path = static_cast<PathNode *>(qry.node);
        if (path->segs.size() == 1) {
            auto seg = static_cast<PathSeg *>(path->segs.front());
            if (seg->nodes.front()->type == kSegDoubleBlot) {
                assert(qry.type == kCondition);
                qry.type = kAny;
            }
        }
    }
    return true;
}
