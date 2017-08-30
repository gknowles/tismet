// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// query.cpp - tismet query
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


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

struct FuncNode : QueryInfo::Node {
    List<Node> args;
};

struct PathNode : QueryInfo::Node {
    List<Node> segs;
};
struct PathSeg : QueryInfo::Node {
    List<Node> nodes;
};
struct SegLiteral : QueryInfo::Node {
    string_view val;
};
struct SegBlot : QueryInfo::Node {
};
struct SegCharChoice : QueryInfo::Node {
    static_assert(sizeof(unsigned long long) == sizeof(uint64_t));
    bitset<256> vals;
};
struct SegStrChoice : QueryInfo::Node {
    List<Node> literals;
};

struct NumNode : QueryInfo::Node {
    double val;
};

const TokenTable::Token s_funcNames[] = {
    { QueryInfo::kFnMaximumAbove, "maximumAbove" },
    { QueryInfo::kFnSum,          "sum" },
};
static_assert(size(s_funcNames) == 
    QueryInfo::kAfterLastFunc - QueryInfo::kBeforeFirstFunc - 1);
const TokenTable s_funcNameTbl{s_funcNames, size(s_funcNames)};

} // namespace


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void appendNode (string & out, const SegStrChoice & node) {
    vector<string_view> literals;
    for (auto && sv : node.literals) {
        literals.push_back(static_cast<const SegLiteral &>(sv).val);
    }
    sort(literals.begin(), literals.end());
    auto it = unique(literals.begin(), literals.end());
    literals.erase(it, literals.end());

    if (literals.size() < 2) {
        if (!literals.empty())
            out += literals.front();
        return;
    }

    out += '{';
    auto ptr = literals.data();
    auto eptr = ptr + literals.size();
    out += *ptr++;
    while (ptr != eptr) {
        out += ',';
        out += *ptr++;
    }
    out += '}';
}

//===========================================================================
static void appendNode (string & out, const QueryInfo::Node & node) {
    size_t first = true;
    switch (node.type) {
    case QueryInfo::kPath:
        for (auto && seg : static_cast<const PathNode &>(node).segs) {
            if (first) {
                first = false;
            } else {
                out += '.';
            }
            appendNode(out, seg);
        }
        break;
    case QueryInfo::kPathSeg:
        for (auto && sn : static_cast<const PathSeg &>(node).nodes) 
            appendNode(out, sn);
        break;
    case QueryInfo::kSegLiteral:
        out += static_cast<const SegLiteral &>(node).val;
        break;
    case QueryInfo::kSegBlot:
        out += '*';
        break;
    case QueryInfo::kSegCharChoice:
        {
        auto & vals = static_cast<const SegCharChoice &>(node).vals;
        out += '[';
        for (unsigned i = 0; i < 256; ++i) {
            if (vals.test(i))
                out += (unsigned char) i;
        }
        out += ']';
        }
        break;
    case QueryInfo::kSegStrChoice:
        appendNode(out, static_cast<const SegStrChoice &>(node));
        break;
    case QueryInfo::kNum:
        {
            ostringstream os;
            os << static_cast<const NumNode &>(node).val;
            out += os.str();
        }
        break;
    default:
        assert(node.type > QueryInfo::kBeforeFirstFunc);
        assert(node.type < QueryInfo::kAfterLastFunc);
        out += tokenTableGetName(s_funcNameTbl, node.type);
        out += '(';
        for (auto && arg : static_cast<const FuncNode &>(node).args) {
            if (first) {
                first = false;
            } else {
                out += ", ";
            }
            appendNode(out, arg);
        }
        out += ')';
        break;
    }
}


/****************************************************************************
*
*   QueryParserBase
*
***/

//===========================================================================
bool QueryParserBase::startFunc (QueryInfo::NodeType type) {
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

//===========================================================================
QueryInfo::Node * addPath(QueryInfo * qi) {
    assert(!qi->node);
    qi->node = qi->heap.emplace<PathNode>();
    qi->node->type = QueryInfo::kPath;
    return qi->node;
}

//===========================================================================
QueryInfo::Node * addSeg(QueryInfo * qi, QueryInfo::Node * node) {
    assert(node->type == QueryInfo::kPath);
    auto path = static_cast<PathNode *>(node);
    path->segs.link(qi->heap.emplace<PathSeg>());
    auto seg = path->segs.back();
    seg->type = QueryInfo::kPathSeg;
    return seg;
}

//===========================================================================
QueryInfo::Node * addSegLiteral(
    QueryInfo * qi, 
    QueryInfo::Node * node, 
    string_view val
) {
    assert(node->type == QueryInfo::kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegLiteral>());
    auto sn = static_cast<SegLiteral *>(seg->nodes.back());
    sn->type = QueryInfo::kSegLiteral;
    sn->val = val;
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegBlot(QueryInfo * qi, QueryInfo::Node * node) {
    assert(node->type == QueryInfo::kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    if (!seg->nodes.empty() && seg->nodes.back()->type == QueryInfo::kSegBlot)
        return nullptr;
    qi->flags |= QueryInfo::fWild;
    seg->nodes.link(qi->heap.emplace<SegBlot>());
    auto sn = static_cast<SegBlot *>(seg->nodes.back());
    sn->type = QueryInfo::kSegBlot;
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegChoices(
    QueryInfo * qi,
    QueryInfo::Node * node, 
    bitset<256> & vals
) {
    assert(node->type == QueryInfo::kPathSeg);
    if (auto cnt = vals.count(); cnt < 2) {
        if (!cnt)
            return nullptr;
        for (unsigned i = 0; i < vals.size(); ++i) {
            if (vals.test(i)) {
                vals.reset(i);
                char * lit = qi->heap.alloc(1, 1);
                *lit = (unsigned char) i;
                return addSegLiteral(qi, node, string_view{lit, 1});
            }
        }
    }
    auto seg = static_cast<PathSeg *>(node);
    qi->flags |= QueryInfo::fWild;
    seg->nodes.link(qi->heap.emplace<SegCharChoice>());
    auto sn = static_cast<SegCharChoice *>(seg->nodes.back());
    sn->type = QueryInfo::kSegCharChoice;
    swap(sn->vals, vals);
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegStrChoices(QueryInfo * qi, QueryInfo::Node * node) {
    assert(node->type == QueryInfo::kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegStrChoice>());
    auto sn = seg->nodes.back();
    sn->type = QueryInfo::kSegStrChoice;
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegChoice(
    QueryInfo * qi,
    QueryInfo::Node * node,
    std::string_view val
) {
    assert(node->type == QueryInfo::kSegStrChoice);
    auto sn = static_cast<SegStrChoice *>(node);
    if (!sn->literals.empty())
        qi->flags |= QueryInfo::fWild;
    sn->literals.link(qi->heap.emplace<SegLiteral>());
    auto sv = static_cast<SegLiteral *>(sn->literals.back());
    sv->type = QueryInfo::kSegLiteral;
    sv->val = val;
    return sv;
}

//===========================================================================
QueryInfo::Node * addFunc(QueryInfo * qi, QueryInfo::NodeType type) {
    assert(!qi->node);
    assert(type > QueryInfo::kBeforeFirstFunc);
    assert(type < QueryInfo::kAfterLastFunc);
    qi->node = qi->heap.emplace<FuncNode>();
    qi->node->type = type;
    return qi->node;
}

//===========================================================================
QueryInfo::Node * addFuncArg(
    QueryInfo * qi, 
    QueryInfo::Node * node, 
    QueryInfo::NodeType type
) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<FuncNode>());
    auto arg = static_cast<FuncNode *>(func->args.back());
    arg->type = type;
    return arg;
}

//===========================================================================
QueryInfo::Node * addPathArg(QueryInfo * qi, QueryInfo::Node * node) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<PathNode>());
    auto arg = static_cast<PathNode *>(func->args.back());
    arg->type = QueryInfo::kPath;
    return arg;
}

//===========================================================================
QueryInfo::Node * addNumArg(
    QueryInfo * qi, 
    QueryInfo::Node * node, 
    double val
) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<NumNode>());
    auto arg = static_cast<NumNode *>(func->args.back());
    arg->type = QueryInfo::kNum;
    arg->val = val;
    return arg;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
bool queryParse(QueryInfo & qry, string_view src) {
    assert(*src.end() == 0);
    qry = {};
    auto ptr = src.data();
    auto parser = QueryParser{&qry};
    if (!parser.parse(ptr)) {
        logParseError("Invalid query", "", parser.errpos(), src);
        return false;
    }

    // normalize
    string text;
    assert(qry.node);
    appendNode(text, *qry.node);
    qry = {};
    qry.text = qry.heap.strdup(text.c_str());
    parser = QueryParser{&qry};
    [[maybe_unused]] bool success = parser.parse(qry.text);
    assert(success);
    return true;
}

//===========================================================================
void queryPathSegments(
    vector<QueryInfo::PathSegment> & out, 
    const QueryInfo & qry
) {
    out.clear();
    if (qry.node->type != QueryInfo::kPath)
        return;
    auto path = static_cast<const PathNode *>(qry.node);
    for (auto && seg : path->segs) {
        QueryInfo::PathSegment si;
        auto & sn = static_cast<const PathSeg &>(seg);
        if (sn.nodes.size() > 1)
            si.flags |= QueryInfo::fWild;
        si.node = &seg;
        auto lit = static_cast<const SegLiteral *>(sn.nodes.front());
        if (lit->type == QueryInfo::kSegLiteral) 
            si.prefix = lit->val;
        out.push_back(si);
    }
}

//===========================================================================
static bool matchSegment(
    const List<QueryInfo::Node> & nodes,
    const QueryInfo::Node * node,
    string_view val
) {
    if (!node)
        return val.empty();

    switch (node->type) {
    case QueryInfo::kSegBlot:
        // TODO: early out of val.size() < minimum required length
        for (; !val.empty(); val.remove_prefix(1)) {
            if (matchSegment(nodes, nodes.next(node), val))
                return true;
        }
        return matchSegment(nodes, nodes.next(node), val);

    case QueryInfo::kSegCharChoice:
        if (val.empty()
            || !static_cast<const SegCharChoice *>(node)->vals.test(val[0])
        ) {
            return false;
        }
        return matchSegment(nodes, nodes.next(node), val.substr(1));

    case QueryInfo::kSegLiteral:
    {
        auto & lit = static_cast<const SegLiteral *>(node)->val;
        auto len = lit.size();
        return lit == val.substr(0, len)
            && matchSegment(nodes, nodes.next(node), val.substr(len));
    }

    case QueryInfo::kSegStrChoice:
        for (auto && sn : static_cast<const SegStrChoice *>(node)->literals) {
            auto & lit = static_cast<const SegLiteral &>(sn).val;
            auto len = lit.size();
            if (lit != val.substr(0, len))
                continue;
            if (matchSegment(nodes, nodes.next(node), val.substr(len)))
                return true;
        }
        return false;

    default:
        assert(0 && "not a path segment node type");
        return false;
    }
}

//===========================================================================
bool queryMatchSegment(
    const QueryInfo::Node * node,
    string_view val
) {
    assert(node->type == QueryInfo::kPathSeg);
    auto & nodes = static_cast<const PathSeg *>(node)->nodes;
    return matchSegment(nodes, nodes.front(), val);
}
