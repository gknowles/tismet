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
    Function::Type func;
    List<Node> args;
};

struct PathNode : Node {
    List<Node> segs;
};
struct PathSeg : Node {
    List<Node> nodes;
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
struct SegStrChoice : Node {
    List<Node> literals;
};

struct NumNode : Node {
    double val;
};
struct StringNode : Node {
    string_view val;
};

const TokenTable::Token s_funcNames[] = {
    { Function::kAlias,                 "alias" },
    { Function::kDerivative,            "derivative" },
    { Function::kHighestCurrent,        "highestCurrent" },
    { Function::kHighestMax,            "highestMax" },
    { Function::kKeepLastValue,         "keepLastValue" },
    { Function::kMaximumAbove,          "maximumAbove" },
    { Function::kNonNegativeDerivative, "nonNegativeDerivative" },
    { Function::kScale,                 "scale" },
    { Function::kSum,                   "sum" },
    { Function::kTimeShift,             "timeShift" },
};
static_assert(size(s_funcNames) == Function::kFuncTypes);
const TokenTable s_funcNameTbl{s_funcNames, size(s_funcNames)};

} // namespace


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void appendNode (string * out, const SegStrChoice & node) {
    vector<string_view> literals;
    for (auto && sv : node.literals) {
        literals.push_back(static_cast<const SegLiteral &>(sv).val);
    }
    sort(literals.begin(), literals.end());
    auto it = unique(literals.begin(), literals.end());
    literals.erase(it, literals.end());

    if (literals.size() < 2) {
        if (!literals.empty())
            out->append(literals.front());
        return;
    }

    out->push_back('{');
    auto ptr = literals.data();
    auto eptr = ptr + literals.size();
    out->append(*ptr++);
    while (ptr != eptr) {
        out->push_back(',');
        out->append(*ptr++);
    }
    out->push_back('}');
}

//===========================================================================
static void appendNode (string * out, const Node & node) {
    size_t first = true;
    switch (node.type) {
    case kPath:
        for (auto && seg : static_cast<const PathNode &>(node).segs) {
            if (first) {
                first = false;
            } else {
                out->push_back('.');
            }
            appendNode(out, seg);
        }
        break;
    case kPathSeg:
        for (auto && sn : static_cast<const PathSeg &>(node).nodes)
            appendNode(out, sn);
        break;
    case kSegLiteral:
        out->append(static_cast<const SegLiteral &>(node).val);
        break;
    case kSegBlot:
        out->push_back('*');
        break;
    case kSegDoubleBlot:
        out->append("**");
        break;
    case kSegCharChoice:
        {
            auto & vals = static_cast<const SegCharChoice &>(node).vals;
            out->push_back('[');
            for (unsigned i = 0; i < 256; ++i) {
                if (vals.test(i))
                    out->push_back((unsigned char) i);
            }
            out->push_back(']');
        }
        break;
    case kSegStrChoice:
        appendNode(out, static_cast<const SegStrChoice &>(node));
        break;
    case kNum:
        out->append(StrFrom<double>(static_cast<const NumNode &>(node).val));
        break;
    case kString:
        out->push_back('"');
        out->append(static_cast<const StringNode &>(node).val);
        out->push_back('"');
        break;
    case kFunc:
        out->append(tokenTableGetName(
            s_funcNameTbl,
            static_cast<const FuncNode &>(node).func
        ));
        out->push_back('(');
        for (auto && arg : static_cast<const FuncNode &>(node).args) {
            if (first) {
                first = false;
            } else {
                out->append(", ");
            }
            appendNode(out, arg);
        }
        out->push_back(')');
        break;
    }
}


/****************************************************************************
*
*   QueryParserBase
*
***/

//===========================================================================
bool QueryParserBase::startFunc (Function::Type type) {
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
Node * addPath(QueryInfo * qi) {
    assert(!qi->node);
    qi->node = qi->heap.emplace<PathNode>();
    qi->node->type = kPath;
    return qi->node;
}

//===========================================================================
static void removeRedundantSegments(PathNode * path) {
    if (path->segs.empty())
        return;

    // Double blot segments are redundant if they are separated by zero
    // or more blot segments.
    auto node = static_cast<PathSeg *>(path->segs.front());
    while (auto next = static_cast<PathSeg *>(path->segs.next(node))) {
        if (node->nodes.front()->type == kSegDoubleBlot) {
            while (next->nodes.size() == 1
                && next->nodes.front()->type == kSegBlot
            ) {
                next = static_cast<PathSeg *>(path->segs.next(next));
                if (!next)
                    return;
            }
            if (next->nodes.front()->type == kSegDoubleBlot)
                path->segs.unlink(node);
        }
        node = next;
    }
}

//===========================================================================
void endPath(QueryInfo * qi, Node * node) {
    assert(node->type == kPath);
    auto path = static_cast<PathNode *>(node);
    removeRedundantSegments(path);
}

//===========================================================================
Node * addSeg(QueryInfo * qi, Node * node) {
    assert(node->type == kPath);
    auto path = static_cast<PathNode *>(node);
    path->segs.link(qi->heap.emplace<PathSeg>());
    auto seg = path->segs.back();
    seg->type = kPathSeg;
    return seg;
}

//===========================================================================
void endSeg(QueryInfo * qi, Node * node) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    if (seg->nodes.size() == 1
        && seg->nodes.front()->type == kSegBlot
    ) {
        auto sn = static_cast<SegBlot *>(seg->nodes.front());
        if (sn->count == 2)
            sn->type = kSegDoubleBlot;
    }
}

//===========================================================================
Node * addSegLiteral(
    QueryInfo * qi,
    Node * node,
    string_view val
) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegLiteral>());
    auto sn = static_cast<SegLiteral *>(seg->nodes.back());
    sn->type = kSegLiteral;
    sn->val = val;
    return sn;
}

//===========================================================================
Node * addSegBlot(QueryInfo * qi, Node * node) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    if (!seg->nodes.empty() && seg->nodes.back()->type == kSegBlot) {
        auto sn = static_cast<SegBlot *>(seg->nodes.back());
        sn->count += 1;
        return nullptr;
    }
    qi->type = kCondition;
    seg->nodes.link(qi->heap.emplace<SegBlot>());
    auto sn = static_cast<SegBlot *>(seg->nodes.back());
    sn->type = kSegBlot;
    sn->count = 1;
    return sn;
}

//===========================================================================
Node * addSegChoices(
    QueryInfo * qi,
    Node * node,
    bitset<256> & vals
) {
    assert(node->type == kPathSeg);
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
    qi->type = kCondition;
    seg->nodes.link(qi->heap.emplace<SegCharChoice>());
    auto sn = static_cast<SegCharChoice *>(seg->nodes.back());
    sn->type = kSegCharChoice;
    swap(sn->vals, vals);
    return sn;
}

//===========================================================================
Node * addSegStrChoices(QueryInfo * qi, Node * node) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegStrChoice>());
    auto sn = seg->nodes.back();
    sn->type = kSegStrChoice;
    return sn;
}

//===========================================================================
Node * addSegChoice(
    QueryInfo * qi,
    Node * node,
    std::string_view val
) {
    assert(node->type == kSegStrChoice);
    auto sn = static_cast<SegStrChoice *>(node);
    if (!sn->literals.empty())
        qi->type = kCondition;
    sn->literals.link(qi->heap.emplace<SegLiteral>());
    auto sv = static_cast<SegLiteral *>(sn->literals.back());
    sv->type = kSegLiteral;
    sv->val = val;
    return sv;
}

//===========================================================================
Node * addFunc(QueryInfo * qi, Function::Type type) {
    assert(!qi->node);
    auto func = qi->heap.emplace<FuncNode>();
    qi->node = func;
    func->type = kFunc;
    func->func = type;
    return func;
}

//===========================================================================
Node * addFuncArg(
    QueryInfo * qi,
    Node * node,
    Function::Type type
) {
    assert(node->type == kFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<FuncNode>());
    auto arg = static_cast<FuncNode *>(func->args.back());
    arg->type = kFunc;
    arg->func = type;
    return arg;
}

//===========================================================================
Node * addPathArg(QueryInfo * qi, Node * node) {
    assert(node->type == kFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<PathNode>());
    auto arg = static_cast<PathNode *>(func->args.back());
    arg->type = kPath;
    return arg;
}

//===========================================================================
Node * addNumArg(
    QueryInfo * qi,
    Node * node,
    double val
) {
    assert(node->type == kFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<NumNode>());
    auto arg = static_cast<NumNode *>(func->args.back());
    arg->type = kNum;
    arg->val = val;
    return arg;
}

//===========================================================================
Node * addStringArg(
    QueryInfo * qi,
    Node * node,
    string_view val
) {
    assert(node->type == kFunc);
    auto func = static_cast<FuncNode *>(node);
    func->args.link(qi->heap.emplace<StringNode>());
    auto arg = static_cast<StringNode *>(func->args.back());
    arg->type = kString;
    arg->val = val;
    return arg;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
bool Query::parse(QueryInfo & qry, string_view src) {
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
    string text;
    appendNode(&text, *qry.node);
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

//===========================================================================
void Query::getPathSegments(
    vector<PathSegment> * out,
    const QueryInfo & qry
) {
    out->clear();
    if (qry.node->type != kPath)
        return;
    auto path = static_cast<const PathNode *>(qry.node);
    for (auto && seg : path->segs) {
        PathSegment si;
        auto & sn = static_cast<const PathSeg &>(seg);
        if (sn.nodes.size() > 1) {
            si.type = kCondition;
        } else {
            assert(sn.nodes.size() == 1);
            auto node = sn.nodes.front();
            if (node->type == kSegBlot) {
                si.type = kAny;
            } else if (node->type == kSegDoubleBlot) {
                si.type = kDynamicAny;
                si.count = 0;
            } else if (node->type == kSegLiteral) {
                si.type = kExact;
            } else {
                si.type = kCondition;
            }
        }
        si.node = &seg;
        auto lit = static_cast<const SegLiteral *>(sn.nodes.front());
        if (lit->type == kSegLiteral)
            si.prefix = lit->val;
        out->push_back(si);
    }
}

//===========================================================================
static MatchResult matchSegment(
    const List<Node> & nodes,
    const Node * node,
    string_view val
) {
    if (!node)
        return val.empty() ? kMatch : kNoMatch;

    switch (node->type) {
    case kSegBlot:
        // TODO: early out of val.size() < minimum required length
        for (; !val.empty(); val.remove_prefix(1)) {
            if (matchSegment(nodes, nodes.next(node), val))
                return kMatch;
        }
        return matchSegment(nodes, nodes.next(node), val);

    case kSegDoubleBlot:
        return kMatchRest;

    case kSegCharChoice:
        if (val.empty()
            || !static_cast<const SegCharChoice *>(node)->vals.test(val[0])
        ) {
            return kNoMatch;
        }
        return matchSegment(nodes, nodes.next(node), val.substr(1));

    case kSegLiteral:
    {
        auto & lit = static_cast<const SegLiteral *>(node)->val;
        auto len = lit.size();
        if (lit != val.substr(0, len))
            return kNoMatch;
        return matchSegment(nodes, nodes.next(node), val.substr(len));
    }

    case kSegStrChoice:
        for (auto && sn : static_cast<const SegStrChoice *>(node)->literals) {
            auto & lit = static_cast<const SegLiteral &>(sn).val;
            auto len = lit.size();
            if (lit != val.substr(0, len))
                continue;
            if (matchSegment(nodes, nodes.next(node), val.substr(len)))
                return kMatch;
        }
        return kNoMatch;

    default:
        assert(0 && "not a path segment node type");
        return kNoMatch;
    }
}

//===========================================================================
MatchResult Query::matchSegment(
    const Node & node,
    string_view val
) {
    assert(node.type == kPathSeg);
    auto & nodes = static_cast<const PathSeg &>(node).nodes;
    return ::matchSegment(nodes, nodes.front(), val);
}

//===========================================================================
NodeType Query::getType(const Node & node) {
    return node.type;
}

//===========================================================================
double Query::getNumber(const Node & node) {
    if (node.type == kNum) {
        return static_cast<const NumNode &>(node).val;
    } else {
        return NAN;
    }
}

//===========================================================================
string_view Query::getString(const Node & node) {
    if (node.type == kString) {
        return static_cast<const StringNode &>(node).val;
    } else {
        return {};
    }
}

//===========================================================================
bool Query::getFunc(
    Function * out,
    const Node & node
) {
    if (node.type != kFunc)
        return false;

    auto & fn = static_cast<const FuncNode &>(node);
    out->type = fn.func;
    out->args.clear();
    for (auto && arg : fn.args)
        out->args.push_back(&arg);
    return true;
}

//===========================================================================
const char * Query::getFuncName(
    Query::Function::Type ftype,
    const char * defVal
) {
    return tokenTableGetName(s_funcNameTbl, ftype, defVal);
}

//===========================================================================
string Query::toString(const Node & node) {
    string out;
    appendNode(&out, node);
    return out;
}
