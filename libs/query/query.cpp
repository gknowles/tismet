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

const TokenTable::Token s_funcNames[] = {
    { Function::kAlias,                 "alias" },
    { Function::kAliasSub,              "aliasSub" },
    { Function::kColor,                 "color" },
    { Function::kDerivative,            "derivative" },
    { Function::kDrawAsInfinite,        "drawAsInfinite" },
    { Function::kHighestCurrent,        "highestCurrent" },
    { Function::kHighestMax,            "highestMax" },
    { Function::kKeepLastValue,         "keepLastValue" },
    { Function::kLegendValue,           "legendValue" },
    { Function::kLineWidth,             "lineWidth" },
    { Function::kMaximumAbove,          "maximumAbove" },
    { Function::kMaxSeries,             "maxSeries" },
    { Function::kMinSeries,             "minSeries" },
    { Function::kMovingAverage,         "movingAverage" },
    { Function::kNonNegativeDerivative, "nonNegativeDerivative" },
    { Function::kRemoveAboveValue,      "removeAboveValue" },
    { Function::kRemoveBelowValue,      "removeBelowValue" },
    { Function::kScale,                 "scale" },
    { Function::kScaleToSeconds,        "scaleToSeconds" },
    { Function::kSum,                   "sum" },
    { Function::kTimeShift,             "timeShift" },
};
static_assert(size(s_funcNames) == Function::kFuncTypes);
const TokenTable s_funcNameTbl{s_funcNames};

} // namespace


/****************************************************************************
*
*   Helpers
*
***/

static bool operator< (const Node & a, const Node & b);

//===========================================================================
static bool operator< (const List<Node> & a, const List<Node> & b) {
    return lexicographical_compare(
        a.begin(),
        a.end(),
        b.begin(),
        b.end(),
        [](auto & a, auto & b) { return a < b; }
    );
}

//===========================================================================
template<size_t N>
static bool operator< (const bitset<N> & a, const bitset<N> & b) {
    return memcmp(&a, &b, sizeof(a)) < 0;
}

//===========================================================================
static bool operator< (const Node & a, const Node & b) {
    if (a.type != b.type)
        return a.type < b.type;

    switch (a.type) {
    case kPath:
        return static_cast<const PathNode &>(a).segs
            < static_cast<const PathNode &>(b).segs;
    case kPathSeg:
        return static_cast<const PathSeg &>(a).nodes
            < static_cast<const PathSeg &>(b).nodes;
    case kSegEmpty:
        return false;
    case kSegLiteral:
        return static_cast<const SegLiteral &>(a).val
            < static_cast<const SegLiteral &>(b).val;
    case kSegBlot:
        return false;
    case kSegDoubleBlot:
        return false;
    case kSegCharChoice:
        return static_cast<const SegCharChoice &>(a).vals
            < static_cast<const SegCharChoice &>(b).vals;
    case kSegSegChoice:
        return static_cast<const SegSegChoice &>(a).segs
            < static_cast<const SegSegChoice &>(b).segs;
    case kNum:
        return static_cast<const NumNode &>(a).val
            < static_cast<const NumNode &>(b).val;
    case kString:
        return static_cast<const StringNode &>(a).val
            < static_cast<const StringNode &>(b).val;
    case kFunc:
        {
            auto & af = static_cast<const FuncNode &>(a);
            auto & bf = static_cast<const FuncNode &>(b);
            if (af.func != bf.func)
                return af.func < bf.func;
            return af.args < bf.args;
        }
    }

    assert(!"Unknown node type");
    return false;
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
    assert(node->type == kPath || node->type == kSegSegChoice);
    auto path = static_cast<PathNode *>(node);
    path->segs.link(qi->heap.emplace<PathSeg>());
    auto seg = path->segs.back();
    seg->type = kPathSeg;
    return seg;
}

//===========================================================================
void endSeg(QueryInfo * qi, Node * node, Node * parent) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    if (parent->type == kPath
        && seg->nodes.size() == 1
        && seg->nodes.front()->type == kSegBlot
    ) {
        auto sn = static_cast<SegBlot *>(seg->nodes.front());
        if (sn->count == 2)
            sn->type = kSegDoubleBlot;
    }
}

//===========================================================================
Node * addSegEmpty(QueryInfo * qi, Node * node) {
    assert(node->type == kPathSeg);
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegEmpty>());
    auto sn = static_cast<SegEmpty *>(seg->nodes.back());
    sn->type = kSegEmpty;
    return sn;
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
Node * addSegCharChoices(
    QueryInfo * qi,
    Node * node,
    bitset<256> & vals
) {
    assert(node->type == kPathSeg);
    if (auto cnt = vals.count(); cnt < 2) {
        if (!cnt)
            return nullptr;
        char * lit = qi->heap.alloc(1, 1);
        for (int i = 0; i < vals.size(); ++i) {
            if (vals.test(i)) {
                *lit = (unsigned char) i;
                return addSegLiteral(qi, node, string_view{lit, 1});
            }
        }
    }

    qi->type = kCondition;
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegCharChoice>());
    auto sn = static_cast<SegCharChoice *>(seg->nodes.back());
    sn->type = kSegCharChoice;
    swap(sn->vals, vals);
    return sn;
}

//===========================================================================
Node * addSegSegChoices(QueryInfo * qi, Node * node) {
    assert(node->type == kPathSeg);
    qi->type = kCondition;
    auto seg = static_cast<PathSeg *>(node);
    seg->nodes.link(qi->heap.emplace<SegSegChoice>());
    auto sn = seg->nodes.back();
    sn->type = kSegSegChoice;
    return sn;
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
*   Conversion to string
*
***/

static void appendNode (string * out, const Node & node);

//===========================================================================
static void appendNode (string * out, const SegSegChoice & node) {
    vector<const PathSeg *> segs;
    for (auto && sn : node.segs) {
        segs.push_back(static_cast<const PathSeg *>(&sn));
    }
    auto cmp = [](auto & a, auto & b) { return *a < *b; };
    sort(segs.begin(), segs.end(), cmp);
    auto it = unique(segs.begin(), segs.end(), not_fn(cmp));
    segs.erase(it, segs.end());

    if (segs.size() < 2) {
        if (!segs.empty())
            appendNode(out, *segs.front());
        return;
    }

    out->push_back('{');
    auto ptr = segs.data();
    auto eptr = ptr + segs.size();
    appendNode(out, **ptr++);
    while (ptr != eptr) {
        out->push_back(',');
        appendNode(out, **ptr++);
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
    case kSegEmpty:
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
            for (auto i = 0; i < vals.size(); ++i) {
                if (vals.test(i))
                    out->push_back((unsigned char) i);
            }
            out->push_back(']');
        }
        break;
    case kSegSegChoice:
        appendNode(out, static_cast<const SegSegChoice &>(node));
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

//===========================================================================
string Query::toString(const Node & node) {
    string out;
    appendNode(&out, node);
    return out;
}


/****************************************************************************
*
*   Matching
*
***/

static MatchResult matchSegment(
    const List<Node> & nodes,
    const Node * node,
    string_view val
);

//===========================================================================
static MatchResult matchSegment(
    const List<Node> & nodes,
    const SegSegChoice * node,
    string_view val
) {
    auto & segs = node->segs;
    // TODO: stop at minimum required length of the following string
    for (int i = 0; i <= val.size(); ++i) {
        for (auto && sn : segs) {
            auto & seg = static_cast<const PathSeg &>(sn);
            if (!matchSegment(seg.nodes, seg.nodes.front(), val.substr(0, i)))
                continue;
            if (matchSegment(nodes, nodes.next(node), val.substr(i)))
                return kMatch;
        }
    }
    return kNoMatch;
}

//===========================================================================
static MatchResult matchSegment(
    const List<Node> & nodes,
    const Node * node,
    string_view val
) {
    auto type = node ? node->type : kSegEmpty;

    switch (type) {
    case kSegEmpty:
        return val.empty() ? kMatch : kNoMatch;

    case kSegBlot:
        // TODO: stop at minimum required length of the following string
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

    case kSegSegChoice:
        return matchSegment(
            nodes,
            static_cast<const SegSegChoice *>(node),
            val
        );

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
    string text = toString(*qry.node);
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
