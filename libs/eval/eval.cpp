// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// eval.cpp - tismet eval
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

namespace {

class SourceNode;

struct SourceRange {
    TimePoint first;
    TimePoint last;

    TimePoint lastUsed;
};

struct SampleList {
    TimePoint first;
    Duration interval;
    uint32_t count{0};
    uint32_t metricId{0};

    // EXTENDS BEYOND END OF STRUCT
    double samples[1];
};

struct ResultInfo {
    shared_ptr<char[]> target;
    shared_ptr<char[]> name;
    shared_ptr<SampleList> samples;
    bool more{false};
};

class ResultNode {
public:
    virtual ~ResultNode();
    virtual void onResult(const ResultInfo & info) = 0;

    int m_unfinished{0};
    vector<shared_ptr<SourceNode>> m_sources;
};

struct ResultRange {
    ResultNode * rn{nullptr};
    TimePoint first;
    TimePoint last;
};

class SourceNode : public ITaskNotify {
public:
    virtual void onStart();
    void onTask() override;

    shared_ptr<char[]> m_source;
    unordered_map<shared_ptr<char[]>, shared_ptr<SampleList>> m_sampleLists;
    vector<SourceRange> m_ranges;

    mutex m_mut;
    vector<ResultRange> m_outputs;
};

class SampleReader : public IDbEnumNotify {
public:
    SampleReader(SourceNode * node);

    shared_ptr<char[]> m_name;
    shared_ptr<SampleList> m_samples;

private:
    bool onDbSeriesStart(
        string_view target,
        uint32_t id,
        string_view name,
        DbSampleType type,
        TimePoint first,
        TimePoint last,
        Duration interval
    ) override;
    bool onDbSample(TimePoint time, double value) override;

    UnsignedSet m_done;
    SourceNode & m_node;
    size_t m_pos{0};
    TimePoint m_time;
    Duration m_interval;
};

struct FuncArg {
    string string;
    double number;
};

class FuncNode : public ResultNode, public SourceNode {
public:
    ~FuncNode ();
    void onResult(const ResultInfo & ri) override;
    void onStart() override;
    void onTask() override;

    Query::Function::Type m_type;
    vector<FuncArg> m_args;
};

class Evaluate : public ResultNode, public ITaskNotify, public ListBaseLink<> {
public:
    void onResult(const ResultInfo & ri) override;
    void onTask() override;

    IEvalNotify * m_notify{nullptr};
    TimePoint m_first;
    TimePoint m_last;
    int m_maxPoints{0};

    mutex m_mut;
    deque<ResultInfo> m_results;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static DbHandle s_db;
static unordered_map<string_view, shared_ptr<SourceNode>> s_sources;
static List<Evaluate> s_execs;


/****************************************************************************
*
*   Helpers
*
***/

struct Overlap {
    TimePoint newFirst;
    size_t newCount;
    size_t newPos;
    size_t copyPos;
    size_t copyFromPos;
    size_t copyCount;
};

//===========================================================================
inline static Overlap getOverlap(
    TimePoint first,
    size_t count,
    TimePoint oldFirst,
    size_t oldCount,
    Duration interval
) {
    Overlap out{};
    if (first >= oldFirst) {
        out.newPos = (first - oldFirst) / interval;
        if (out.newPos > oldCount) {
            // entirely after old range
            out.newFirst = first;
            out.newPos = 0;
            out.newCount = count;
        } else if (out.newPos + count <= oldCount) {
            // within old range
            out.newFirst = oldFirst;
            out.newCount = oldCount;
            out.copyCount = oldCount;
        } else {
            // starts within old range but extends after it
            out.newFirst = oldFirst;
            out.newCount = out.newPos + count;
            out.copyCount = out.newPos;
        }
    } else {
        size_t pos = (oldFirst - first) / interval;
        if (count < pos + oldCount) {
            // ends within old range
            if (pos > count) {
                // entirely before old range
                out.newFirst = first;
                out.newCount = count;
            } else {
                // starts before old range and ends within it
                out.newFirst = first;
                out.newCount = pos + oldCount;
                out.copyPos = count;
                out.copyFromPos = count - pos;
                out.copyCount = oldCount - out.copyFromPos;
            }
        } else {
            // complete superset of old range
            out.newFirst = first;
            out.newCount = count;
        }
    }
}

//===========================================================================
static void addOutput(
    shared_ptr<SourceNode> src,
    ResultNode * rn,
    TimePoint first,
    TimePoint last
) {
    auto si = lower_bound(rn->m_sources.begin(), rn->m_sources.end(), src);
    rn->m_sources.insert(si, src);

    scoped_lock<mutex> lk{src->m_mut};
    auto ri = lower_bound(
        src->m_ranges.begin(),
        src->m_ranges.end(),
        first,
        [](auto & a, auto & b){ return a.last < b; }
    );
    if (ri != src->m_ranges.end()
        && first >= ri->first && last <= ri->last
    ) {
        ResultInfo info;
        info.target = src->m_source;
        info.more = true;
        for (auto && [name, slist] : src->m_sampleLists) {
            info.name = name;
            info.samples = slist;
            rn->onResult(info);
        }
        info.name = {};
        info.samples = {};
        info.more = false;
        rn->onResult(info);
        return;
    }

    src->m_outputs.push_back({rn, first, last});
    if (src->m_outputs.size() == 1)
        src->onStart();
}

//===========================================================================
static void remoteOutput(SourceNode * src, ResultNode * rn) {
    scoped_lock<mutex> lk{src->m_mut};
    auto i = src->m_outputs.begin(),
        ei = src->m_outputs.end();
    while (i != ei) {
        if (i->rn == rn) {
            i = src->m_outputs.erase(i);
        } else {
            i += 1;
        }
    }
}

//===========================================================================
static shared_ptr<char[]> toSharedString(string_view src) {
    auto sp = shared_ptr<char[]>(new char[src.size() + 1]);
    memcpy(sp.get(), src.data(), src.size());
    sp[src.size()] = 0;
    return sp;
}

//===========================================================================
static bool addSource(
    ResultNode * rn,
    string_view srcv,
    TimePoint first,
    TimePoint last
) {
    if (auto si = s_sources.find(srcv); si != s_sources.end()) {
        addOutput(si->second, rn, first, last);
        return true;
    }

    auto src = toSharedString(srcv);
    Query::QueryInfo qi;
    if (!Query::parse(qi, src.get()))
        return false;

    auto type = Query::getType(*qi.node);
    if (type == Query::kPath) {
        auto sn = make_shared<SourceNode>();
        sn->m_source = src;
        s_sources[src.get()] = sn;
        addOutput(sn, rn, first, last);
        return true;
    }

    assert(type == Query::kFunc);
    Query::Function qf;
    auto en = make_shared<FuncNode>();
    en->m_source = src;
    if (!Query::getFunction(&qf, *qi.node))
        return false;
    en->m_type = qf.type;
    en->m_unfinished = (int) qf.args.size();
    s_sources[src.get()] = en;
    ResultInfo info;
    info.target = src;
    for (auto && arg : qf.args) {
        switch (Query::getType(*arg)) {
        case Query::kPath:
        case Query::kFunc:
            if (!addSource(en.get(), toString(*arg), first, last))
                return false;
            break;
        case Query::kNum:
            en->m_args.emplace_back().number = Query::getNumber(*arg);
            en->onResult(info);
            break;
        case Query::kString:
            en->m_args.emplace_back().string = Query::getString(*arg);
            en->onResult(info);
            break;
        default:
            return false;
        }
    }
    addOutput(en, rn, first, last);
    return true;
}


/****************************************************************************
*
*   SourceNode
*
***/

//===========================================================================
void SourceNode::onStart() {
    taskPushCompute(this);
}

//===========================================================================
void SourceNode::onTask() {
    TimePoint first = TimePoint::max();
    TimePoint last = {};
    {
        scoped_lock<mutex> lk{m_mut};
        if (m_outputs.empty())
            return;
        for (auto && rr : m_outputs) {
            if (rr.first < first)
                first = rr.first;
            if (rr.last > last)
                last = rr.last;
        }
    }
    UnsignedSet ids;
    dbFindMetrics(ids, s_db, m_source.get());
    ResultInfo info;
    info.target = m_source;
    if (ids.empty()) {
        scoped_lock<mutex> lk{m_mut};
        for (auto && rr : m_outputs)
            rr.rn->onResult(info);
        m_outputs.clear();
        return;
    }

    SampleReader reader(this);
    auto i = ids.begin(),
        ei = ids.end();
    UnsignedSet done;
    for (;;) {
        auto id = *i;
        bool more = (++i != ei);
        if (!done.insert(id)) {
            if (more)
                continue;
            info.more = more;
            info.name = {};
            info.samples = {};
        } else {
            dbEnumSamples(&reader, s_db, id, first, last);
            info.more = more;
            info.name = reader.m_name;
            info.samples = reader.m_samples;
        }

        scoped_lock<mutex> lk{m_mut};
        for (auto && rr : m_outputs)
            rr.rn->onResult(info);
        if (!more) {
            m_outputs.clear();
            return;
        }
    }
}


/****************************************************************************
*
*   SampleReader
*
***/

//===========================================================================
static shared_ptr<SampleList> allocSampleList(
    TimePoint first,
    Duration interval,
    size_t count
) {
    auto vptr = new char[
        offsetof(SampleList, samples) + count * sizeof(*SampleList::samples)
    ];
    auto list = new(vptr) SampleList{first, interval, (uint32_t) count};
    return shared_ptr<SampleList>(list);
}

//===========================================================================
SampleReader::SampleReader(SourceNode * node)
    : m_node(*node)
{}

//===========================================================================
bool SampleReader::onDbSeriesStart(
    string_view target,
    uint32_t id,
    string_view name,
    DbSampleType type,
    TimePoint first,
    TimePoint last,
    Duration interval
) {
    auto count = (last - first) / interval;
    if (!count)
        return false;
    m_name = toSharedString(name);
    m_samples = allocSampleList(first, interval, count);
    m_samples->metricId = id;
    m_pos = 0;
    m_time = first;
    m_interval = interval;
    return true;
}

//===========================================================================
bool SampleReader::onDbSample(TimePoint time, double value) {
    for (; m_time < time; m_time += m_interval, ++m_pos)
        m_samples->samples[m_pos] = NAN;
    m_samples->samples[m_pos] = value;
    m_time += m_interval;
    m_pos += 1;
    return true;
}


/****************************************************************************
*
*   ResultNode
*
***/

//===========================================================================
ResultNode::~ResultNode() {
    for (auto && sn : m_sources)
        remoteOutput(sn.get(), this);
}


/****************************************************************************
*
*   FuncNode
*
***/

//===========================================================================
FuncNode::~FuncNode() {
    assert(m_outputs.empty());
}

//===========================================================================
void FuncNode::onStart()
{}

//===========================================================================
void FuncNode::onResult(const ResultInfo & info) {
    assert(!"Functions not implemented");
}

//===========================================================================
void FuncNode::onTask() {
}


/****************************************************************************
*
*   Evaluate
*
***/

//===========================================================================
void Evaluate::onResult(const ResultInfo & info) {
    scoped_lock<mutex> lk{m_mut};
    m_results.push_back(info);
    if (m_results.size() == 1)
        taskPushCompute(this);
}

//===========================================================================
void Evaluate::onTask() {
    unique_lock<mutex> lk{m_mut};
    assert(!m_results.empty());
    for (;;) {
        auto info = m_results.front();
        lk.unlock();
        if (info.samples) {
            auto first = info.samples->first;
            auto last = first + info.samples->count * info.samples->interval;
            if (!m_notify->onDbSeriesStart(
                info.target.get(),
                info.samples->metricId,
                info.name.get(),
                kSampleTypeFloat64,
                first,
                last,
                info.samples->interval
            )) {
                goto NEXT_RESULT;
            }
            auto samp = info.samples->samples;
            for (; first < last; first += info.samples->interval, ++samp) {
                if (!m_notify->onDbSample(first, *samp))
                    goto NEXT_RESULT;
            }
            m_notify->onDbSeriesEnd(info.samples->metricId);
        }

    NEXT_RESULT:
        if (!info.more && !--m_unfinished) {
            m_notify->onEvalEnd();
            delete this;
            return;
        }
        lk.lock();
        m_results.pop_front();
        if (m_results.empty())
            return;
    }
}


/****************************************************************************
*
*   Shutdown monitor
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    assert(s_execs.empty());
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void evalInitialize(DbHandle f) {
    shutdownMonitor(&s_cleanup);
    s_db = f;
}

//===========================================================================
void execAdd(
    IEvalNotify * notify,
    const vector<string_view> & targets,
    TimePoint first,
    TimePoint last,
    size_t maxPoints
) {
    auto ex = new Evaluate;
    s_execs.link(ex);
    ex->m_notify = notify;
    ex->m_unfinished = (int) targets.size();
    ex->m_first = first;
    ex->m_last = last;
    ex->m_maxPoints = (int) maxPoints;
    for (auto && target : targets) {
        if (!addSource(ex, target, first, last)) {
            delete ex;
            notify->onEvalError("Invalid target parameter: " + string(target));
            return;
        }
    }
}
