// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// eval.cpp - tismet eval
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;


/****************************************************************************
*
*   Private
*
***/

namespace {

class DbDataNode : public SourceNode, ITaskNotify, IDbDataNotify {
private:
    void readMore();

    void onSourceStart() override;
    void onTask() override;

    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(uint32_t id, TimePoint time, double value) override;
    void onDbSeriesEnd(uint32_t id) override;

    SourceContext m_context;

    // Used in the onDb*() callbacks to determine if it's a synchronous callback
    // from an active readMore() loop or an async callback resuming activity
    // from another thread. This is used to decide if a new readMore loop
    // needs to be started.
    thread::id m_readTid;

    ResultInfo m_result;
    UnsignedSet m_unfinishedIds;

    size_t m_pos{0};
    TimePoint m_time;
};

class Evaluate : public ResultNode, public ListBaseLink<> {
public:
    Evaluate();
    ~Evaluate();

    void onTask() override;

    // Returns false when done receiving results, either normally or because
    // it was aborted.
    bool onEvalApply(ResultInfo & info);

    IEvalNotify * m_notify{};
    DbContextHandle m_ctx;
    TimePoint m_first;
    TimePoint m_last;
    Duration m_minInterval{};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static DbHandle s_db;

static shared_mutex s_mut;
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
static shared_ptr<char[]> toSharedString(string_view src) {
    auto sp = shared_ptr<char[]>(new char[src.size() + 1]);
    memcpy(sp.get(), src.data(), src.size());
    sp[src.size()] = 0;
    return sp;
}

//===========================================================================
static shared_ptr<SourceNode> addSource(
    ResultNode * rn,
    shared_ptr<SourceNode> src,
    bool updateGlobal = true
) {
    if (updateGlobal) {
        scoped_lock lk{s_mut};
        auto ib = s_sources.insert({src->sourceName().get(), src});
        if (!ib.second)
            src = ib.first->second;
    }
    rn->m_sources.push_back(src);
    return src;
}

//===========================================================================
static shared_ptr<SourceNode> addSource(ResultNode * rn, string_view srcv) {
    shared_lock lk{s_mut};
    if (auto si = s_sources.find(srcv); si != s_sources.end())
        return addSource(rn, si->second, false);
    lk.unlock();

    auto src = toSharedString(srcv);
    Query::QueryInfo qi;
    if (!parse(qi, src.get()))
        return {};

    auto type = Query::getType(*qi.node);
    if (type == Query::kPath) {
        auto sn = make_shared<DbDataNode>();
        sn->init(src);
        return addSource(rn, sn);
    }

    assert(type == Query::kFunc);
    Query::Function qf;
    if (!Query::getFunc(&qf, *qi.node))
        return {};
    shared_ptr<FuncNode> fnode;
    if (auto instance = funcCreate(qf.type)) {
        fnode = shared_ptr<FuncNode>(
            new FuncNode,
            RefCount::Deleter{}
        );
        fnode->init(src, move(instance));
    } else {
        assert(!"Unsupported function");
        return {};
    }
    if (!fnode->bind(qf.args))
        return {};
    return addSource(rn, fnode);
}


/****************************************************************************
*
*   SourceNode
*
***/

//===========================================================================
SourceNode::~SourceNode() {
    assert(m_outputs.empty());
    assert(m_pendingOutputs.empty());
}

//===========================================================================
void SourceNode::init(shared_ptr<char[]> name) {
    m_source = name;
}

//===========================================================================
void SourceNode::addOutput(const SourceContext & context) {
    unique_lock lk{m_outMut};
    m_pendingOutputs.push_back(context);
    if (m_pendingOutputs.size() != 1 || !m_outputs.empty())
        return;

    lk.unlock();
    onSourceStart();
}

//===========================================================================
void SourceNode::removeOutput(ResultNode * rn) {
    auto pred = [=](auto & a) { return a.rn == rn; };
    scoped_lock lk{m_outMut};
    if (!m_outputs.empty())
        erase_unordered_if(m_outputs, pred);
    if (!m_outputs.empty())
        erase_unordered_if(m_pendingOutputs, pred);
}

//===========================================================================
bool SourceNode::outputContext(SourceContext * out) {
    out->first = TimePoint::max();
    out->last = TimePoint::min();
    out->pretime = {};
    out->presamples = 0;
    out->minInterval = {};
    out->method = {};

    scoped_lock lk{m_outMut};
    if (m_outputs.empty()) {
        if (m_pendingOutputs.empty())
            return false;
        m_outputs.swap(m_pendingOutputs);
    }
    for (auto && ctx : m_outputs) {
        out->first = min(out->first, ctx.first);
        out->last = max(out->last, ctx.last);
        out->pretime = max(out->pretime, ctx.pretime);
        out->presamples = max(out->presamples, ctx.presamples);
    }
    return true;
}

//===========================================================================
void SourceNode::outputResult(const ResultInfo & info) {
    unique_lock lk{m_outMut};
    outputResultImpl_LK(info);
    if (!info.samples) {
        m_outputs.clear();
        if (!m_pendingOutputs.empty()) {
            lk.unlock();
            onSourceStart();
        }
    }
}

//===========================================================================
void SourceNode::outputResultImpl_LK(const ResultInfo & info) {
    if (m_outputs.empty())
        return;
    if (!info.samples) {
        for (auto && ctx : m_outputs)
            ctx.rn->onResult(info);
        return;
    }

    auto i = m_outputs.begin();
    auto e = m_outputs.end();
    sort(i, e, [](auto & a, auto & b) {
        return a.minInterval < b.minInterval;
    });
    auto baseInterval = info.samples->interval;
    ResultInfo out{info};
    while (i->minInterval <= baseInterval) {
        out.argPos = i->argPos;
        i->rn->onResult(out);
        if (++i == e)
            return;
    }
    for (;;) {
        baseInterval = i->minInterval;
        ResultInfo out{info};
        out.samples = reduce(info.samples, baseInterval, info.method);
        for (;;) {
            out.argPos = i->argPos;
            i->rn->onResult(out);
            if (++i == e)
                return;
            if (i->minInterval != baseInterval)
                break;
        }
    }
}


/****************************************************************************
*
*   DbDataNode
*
***/

//===========================================================================
void DbDataNode::onSourceStart() {
    taskPushCompute(this);
}

//===========================================================================
void DbDataNode::onTask() {
    if (!outputContext(&m_context))
        return;

    assert(m_readTid != this_thread::get_id());
    assert(!m_unfinishedIds);
    m_result = {};
    m_result.target = sourceName();
    dbFindMetrics(&m_unfinishedIds, s_db, m_result.target.get());
    readMore();
}

//===========================================================================
void DbDataNode::readMore() {
    m_readTid = this_thread::get_id();

    for (auto finished = !m_unfinishedIds; !finished; ) {
        auto id = m_unfinishedIds.pop_front();

        // Capture whether there are still unfinished ids before calling
        // dbGetSamples(). This is because dbGetSamples() may queue the
        // execution to a different thread, and then that thread could complete
        // the results, start pending outputs, and make a new set of unfinished
        // ids, all before dbGetSamples returns.
        finished = !m_unfinishedIds;

        if (!dbGetSamples(
            this,
            s_db,
            id,
            m_context.first - m_context.pretime,
            m_context.last,
            m_context.presamples
        )) {
            // Request was queued. We don't reset the read tid so when the
            // callback runs it knows it was call from outside of readMore()
            // and must call it again to process the rest of the unfinished.
            return;
        }
    }

    m_readTid = thread::id{};
    m_result.name = {};
    m_result.samples = {};
    outputResult(m_result);
}

//===========================================================================
bool DbDataNode::onDbSeriesStart(const DbSeriesInfo & info) {
    if (!info.type)
        return true;

    auto first = m_context.first
        - m_context.pretime
        - m_context.presamples * info.interval;
    first -= first.time_since_epoch() % info.interval;
    auto last = m_context.last + info.interval;
    last -= last.time_since_epoch() % info.interval;
    auto count = (last - first) / info.interval;
    assert(info.first == info.last || first <= info.first && last >= info.last);
    if (!count)
        return true;

    m_result.name = toSharedString(info.name);
    m_result.samples = SampleList::alloc(first, info.interval, count);
    m_result.samples->metricId = info.id;
    m_pos = 0;
    m_time = first;
    return true;
}

//===========================================================================
bool DbDataNode::onDbSample(uint32_t id, TimePoint time, double value) {
    auto samples = m_result.samples->samples;
    auto interval = m_result.samples->interval;
    for (; m_time < time; m_time += interval, ++m_pos)
        samples[m_pos] = NAN;
    if (m_pos < m_result.samples->count) {
        samples[m_pos] = value;
        m_time += interval;
        m_pos += 1;
    }
    return true;
}

//===========================================================================
void DbDataNode::onDbSeriesEnd(uint32_t id) {
    if (m_result.samples) {
        auto samples = m_result.samples->samples;
        auto interval = m_result.samples->interval;
        auto count = m_result.samples->count;
        auto last = m_result.samples->first + count * interval;
        for (; m_time < last; m_time += interval, ++m_pos)
            samples[m_pos] = NAN;
        assert(m_pos == count);
    }
    auto out = m_result;
    outputResult(out);

    if (m_unfinishedIds && m_readTid != this_thread::get_id()) {
        // Called from a new thread without an active readMore loop, start
        // a new loop to process the rest of the unfinished ids.
        readMore();
    }
}


/****************************************************************************
*
*   ResultNode
*
***/

//===========================================================================
ResultNode::~ResultNode() {
    stopSources();
}

//===========================================================================
void ResultNode::stopSources() {
    for (auto && sn : m_sources)
        sn->removeOutput(this);
}

//===========================================================================
void ResultNode::onResult(const ResultInfo & info) {
    scoped_lock lk{m_resMut};
    m_results.push_back(info);
    if (m_results.size() == 1) {
        incRef();
        taskPushCompute(this);
    }
}


/****************************************************************************
*
*   FuncNode
*
***/

//===========================================================================
void FuncNode::init(
    shared_ptr<char[]> sourceName,
    unique_ptr<IFuncInstance> instance
) {
    incRef();
    m_instance = move(instance);
    SourceNode::init(sourceName);
}

//===========================================================================
bool FuncNode::bind(vector<const Query::Node *> & args) {
    auto ptr = m_instance->onFuncBind(this, args);
    if (ptr != m_instance.get())
        m_instance.reset(ptr);
    return (bool) m_instance;
}

//===========================================================================
void FuncNode::onSourceStart() {
    SourceContext context;
    if (!outputContext(&context))
        return;

    m_instance->onFuncAdjustContext(&context);
    m_unfinished = (int) m_sources.size();
    context.rn = this;
    context.argPos = 0;
    for (auto && sn : m_sources) {
        sn->addOutput(context);
        context.argPos += 1;
    }
}

//===========================================================================
void FuncNode::onTask() {
    unique_lock lk{m_resMut};
    decRef();
    assert(!m_results.empty());
    auto stop = false;
    for (;;) {
        auto info = m_results.front();
        bool more = info.samples || --m_unfinished;
        if (info.samples || !more) {
            lk.unlock();
            stop = !m_instance->onFuncApply(this, info);
            if (stop)
                stopSources();
            lk.lock();
        }
        if (stop) {
            m_results.clear();
        } else {
            m_results.pop_front();
        }
        if (m_results.empty()) {
            if (!more)
                onSourceStart();
            return;
        }
    }
}

//===========================================================================
bool FuncNode::onFuncSource(const Query::Node & node) {
    assert(getType(node) == Query::kFunc || getType(node) == Query::kPath);
    return (bool) addSource(this, toString(node));
}

//===========================================================================
void FuncNode::onFuncOutput(ResultInfo & info) {
    outputResult(info);
}


/****************************************************************************
*
*   Evaluate
*
***/

//===========================================================================
Evaluate::Evaluate() {
    unique_lock lk{s_mut};
    s_execs.link(this);
}

//===========================================================================
Evaluate::~Evaluate() {
    dbCloseContext(m_ctx);

    unique_lock lk{s_mut};
    s_execs.unlink(this);
}

//===========================================================================
void Evaluate::onTask() {
    unique_lock lk{m_resMut};
    decRef();
    assert(!m_results.empty());
    for (;;) {
        auto info = m_results.front();
        lk.unlock();
        auto more = info.samples || --m_unfinished;
        if (info.samples || !more) {
            if (!onEvalApply(info)) {
                decRef();
                return;
            }
        }
        lk.lock();
        m_results.pop_front();
        if (m_results.empty())
            return;
    }
}

//===========================================================================
bool Evaluate::onEvalApply(ResultInfo & info) {
    if (info.samples) {
        DbSeriesInfo dsi;
        dsi.target = info.target.get();
        dsi.id = info.samples->metricId;
        dsi.name = info.name.get();
        dsi.type = kSampleTypeFloat64;
        dsi.interval = info.samples->interval;
        dsi.first = m_first - m_first.time_since_epoch() % dsi.interval;
        dsi.last = m_last
            + dsi.interval
            - m_last.time_since_epoch() % dsi.interval;
        auto presamples = (dsi.first - info.samples->first) / dsi.interval;
        [[maybe_unused]] auto count = (dsi.last - dsi.first) / dsi.interval;
        assert(presamples >= 0);
        assert(count <= info.samples->count - presamples);
        if (!m_notify->onDbSeriesStart(dsi)) {
            m_notify->onEvalEnd();
            return false;
        }
        auto samp = info.samples->samples + presamples;
        auto time = dsi.first;
        for (; time < dsi.last; time += dsi.interval, ++samp) {
            if (!m_notify->onDbSample(dsi.id, time, *samp)) {
                m_notify->onEvalEnd();
                return false;
            }
        }
        m_notify->onDbSeriesEnd(dsi.id);
        return true;
    }

    m_notify->onEvalEnd();
    return false;
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
    shared_lock lk{s_mut};
    assert(!s_execs);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void evalInitialize(DbHandle f) {
    funcInitialize();
    shutdownMonitor(&s_cleanup);
    s_db = f;
}

//===========================================================================
void evaluate(
    IEvalNotify * notify,
    string_view target,
    TimePoint first,
    TimePoint last,
    size_t maxPoints
) {
    auto hostage = RefPtr<Evaluate>(new Evaluate);
    auto ex = hostage.get();
    ex->m_notify = notify;
    ex->m_ctx = dbOpenContext(s_db);
    ex->m_unfinished = 1;
    ex->m_first = first;
    ex->m_last = last;
    if (maxPoints)
        ex->m_minInterval = (last - first) / maxPoints;
    auto sn = addSource(ex, target);
    if (!sn) {
        notify->onEvalError("Invalid target parameter: " + string(target));
        return;
    }
    hostage.release();

    SourceNode::SourceContext context;
    context.rn = ex;
    context.first = first;
    context.last = last;
    context.minInterval = ex->m_minInterval;
    sn->addOutput(context);
}

//===========================================================================
string toString(const Query::Node & node) {
    return toString(node, &funcTokenConv());
}

//===========================================================================
bool parse(Query::QueryInfo & qry, std::string_view src) {
    return parse(qry, src, &funcTokenConv());
}
