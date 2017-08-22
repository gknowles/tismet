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
    vector<unique_ptr<Node>> args;
};

struct PathNode : QueryInfo::Node {
    vector<unique_ptr<Node>> segs;
};
struct PathSeg : QueryInfo::Node {
    vector<unique_ptr<Node>> nodes;
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
    vector<string_view> vals;
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
            appendNode(out, *seg);
        }
        break;
    case QueryInfo::kPathSeg:
        for (auto && sn : static_cast<const PathSeg &>(node).nodes) 
            appendNode(out, *sn);
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
        out += '{';
        for (auto && val : static_cast<const SegStrChoice &>(node).vals) {
            if (first) {
                first = false;
            } else {
                out += ',';
            }
            out += val;
        }
        out += '}';
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
            appendNode(out, *arg);
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
        : addFuncArg(m_nodes.back(), type);
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
    qi->node = make_unique<PathNode>();
    qi->node->type = QueryInfo::kPath;
    return qi->node.get();
}

//===========================================================================
QueryInfo::Node * addSeg(QueryInfo::Node * node) {
    assert(node->type == QueryInfo::kPath);
    auto path = static_cast<PathNode *>(node);
    path->segs.push_back(make_unique<PathSeg>());
    auto seg = path->segs.back().get();
    seg->type = QueryInfo::kPathSeg;
    return seg;
}

//===========================================================================
QueryInfo::Node * addSegLiteral(QueryInfo::Node * node, string_view val) {
    assert(node->type == QueryInfo::kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.push_back(make_unique<SegLiteral>());
    auto sn = static_cast<SegLiteral *>(seg->nodes.back().get());
    sn->type = QueryInfo::kSegLiteral;
    sn->val = val;
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegBlot(QueryInfo::Node * node) {
    assert(node->type == QueryInfo::kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.push_back(make_unique<SegBlot>());
    auto sn = static_cast<SegBlot *>(seg->nodes.back().get());
    sn->type = QueryInfo::kSegBlot;
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegChoice(
    QueryInfo::Node * node, 
    bitset<256> && vals
) {
    assert(node->type == QueryInfo::kPathSeg);
    if (vals.none())
        return nullptr;
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.push_back(make_unique<SegCharChoice>());
    auto sn = static_cast<SegCharChoice *>(seg->nodes.back().get());
    sn->type = QueryInfo::kSegCharChoice;
    swap(sn->vals, vals);
    return sn;
}

//===========================================================================
QueryInfo::Node * addSegChoice(
    QueryInfo::Node * node,
    vector<string_view> && vals
) {
    assert(node->type == QueryInfo::kPathSeg);
    if (vals.empty())
        return nullptr;
    sort(vals.begin(), vals.end());
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.push_back(make_unique<SegStrChoice>());
    auto sn = static_cast<SegStrChoice *>(seg->nodes.back().get());
    sn->type = QueryInfo::kSegStrChoice;
    swap(sn->vals, vals);
    return sn;
}

//===========================================================================
QueryInfo::Node * addFunc(QueryInfo * qi, QueryInfo::NodeType type) {
    assert(!qi->node);
    assert(type > QueryInfo::kBeforeFirstFunc);
    assert(type < QueryInfo::kAfterLastFunc);
    qi->node = make_unique<FuncNode>();
    qi->node->type = type;
    return qi->node.get();
}

//===========================================================================
QueryInfo::Node * addFuncArg(
    QueryInfo::Node * node, 
    QueryInfo::NodeType type
) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.push_back(make_unique<FuncNode>());
    auto arg = static_cast<FuncNode *>(func->args.back().get());
    arg->type = type;
    return arg;
}

//===========================================================================
QueryInfo::Node * addPathArg(QueryInfo::Node * node) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.push_back(make_unique<PathNode>());
    auto arg = static_cast<PathNode *>(func->args.back().get());
    arg->type = QueryInfo::kPath;
    return arg;
}

//===========================================================================
QueryInfo::Node * addNumArg(QueryInfo::Node * node, double val) {
    assert(node->type > QueryInfo::kBeforeFirstFunc);
    assert(node->type < QueryInfo::kAfterLastFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.push_back(make_unique<NumNode>());
    auto arg = static_cast<NumNode *>(func->args.back().get());
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
    QueryParser parser(&qry);
    if (parser.parse(ptr)) 
        return true;
    logParseError("Invalid query", "", parser.errpos(), src);
    return false;
}

//===========================================================================
void queryNormalize(QueryInfo & qry) {
    string text;
    if (qry.node)
        appendNode(text, *qry.node);
    [[maybe_unused]] bool success = queryParse(qry, text);
    assert(success);
    qry.text = move(text);
}
