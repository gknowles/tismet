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

    ResultRange m_range;

    thread::id m_tid;
    ResultInfo m_result;
    UnsignedSet m_unfinished;

    size_t m_pos{0};
    TimePoint m_time;
};

class Evaluate : public ResultNode, public ListBaseLink<> {
public:
    Evaluate();
    ~Evaluate();

    void onTask() override;

    // Return false when done receiving results, either normally or because
    // it was aborted.
    bool onEvalApply(ResultInfo & info);

    IEvalNotify * m_notify{nullptr};
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
static IFactory<FuncNode> * s_funcFacts[Query::Function::kFuncTypes];


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
    shared_ptr<SourceNode> src
) {
    rn->m_sources.push_back(src);
    return src;
}

//===========================================================================
static shared_ptr<SourceNode> addSource(ResultNode * rn, string_view srcv) {
    shared_lock lk{s_mut};
    if (auto si = s_sources.find(srcv); si != s_sources.end())
        return addSource(rn, si->second);
    lk.unlock();

    auto src = toSharedString(srcv);
    Query::QueryInfo qi;
    if (!Query::parse(qi, src.get()))
        return {};

    auto type = Query::getType(*qi.node);
    if (type == Query::kPath) {
        auto sn = make_shared<DbDataNode>();
        sn->init(src);
        {
            scoped_lock lk{s_mut};
            s_sources[src.get()] = sn;
        }
        return addSource(rn, sn);
    }

    assert(type == Query::kFunc);
    Query::Function qf;
    if (!Query::getFunc(&qf, *qi.node))
        return {};
    shared_ptr<FuncNode> fnode;
    if (auto fact = s_funcFacts[qf.type]) {
        fnode = shared_ptr<FuncNode>(
            fact->onFactoryCreate().release(),
            RefCount::Deleter{}
        );
        fnode->init(src);
    } else {
        assert(!"Unsupported function");
        return {};
    }
    {
        scoped_lock lk{s_mut};
        s_sources[src.get()] = fnode;
    }
    vector<FuncArg> fargs;
    for (auto && arg : qf.args) {
        switch (Query::getType(*arg)) {
        case Query::kPath:
        case Query::kFunc:
            if (!addSource(fnode.get(), toString(*arg)))
                return {};
            break;
        case Query::kNum:
            fargs.emplace_back().number = Query::getNumber(*arg);
            break;
        case Query::kString:
            fargs.emplace_back().string =
                toSharedString(Query::getString(*arg));
            break;
        default:
            return {};
        }
    }
    if (!fnode->bind(move(fargs)))
        return {};
    return addSource(rn, fnode);
}


/****************************************************************************
*
*   SampleList
*
***/

//===========================================================================
// static
shared_ptr<SampleList> SampleList::alloc(
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
// static
shared_ptr<SampleList> SampleList::alloc(const SampleList & samples) {
    return alloc(samples.first, samples.interval, samples.count);
}

//===========================================================================
// static
shared_ptr<SampleList> SampleList::dup(const SampleList & samples) {
    auto out = alloc(samples);
    memcpy(
        out->samples,
        samples.samples,
        out->count * sizeof(*out->samples)
    );
    return out;
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
void SourceNode::addOutput(const ResultRange & rr) {
    unique_lock lk{m_outMut};
    m_pendingOutputs.push_back(rr);
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
bool SourceNode::outputRange(ResultRange * out) {
    out->first = TimePoint::max();
    out->last = TimePoint::min();
    out->pretime = {};
    out->presamples = 0;

    scoped_lock lk{m_outMut};
    if (m_outputs.empty()) {
        if (m_pendingOutputs.empty())
            return false;
        m_outputs.swap(m_pendingOutputs);
    }
    for (auto && rr : m_outputs) {
        out->first = min(out->first, rr.first);
        out->last = max(out->last, rr.last);
        out->pretime = max(out->pretime, rr.pretime);
        out->presamples = max(out->presamples, rr.presamples);
    }
    return true;
}

//===========================================================================
static shared_ptr<SampleList> consolidateAvg(
    shared_ptr<SampleList> samples,
    Duration minInterval
) {
    auto baseInterval = samples->interval;
    if (baseInterval >= minInterval)
        return samples;

    auto sps = (minInterval.count() + baseInterval.count() - 1)
        / baseInterval.count();
    auto maxInterval = sps * baseInterval;
    auto first = samples->first;
    first -= first.time_since_epoch() % maxInterval;
    auto presamples = (samples->first - first) / baseInterval;
    auto count = samples->count;
    auto out = SampleList::alloc(
        first,
        maxInterval,
        (count + presamples + sps - 1) / sps
    );

    auto sum = 0.0;
    auto num = presamples;
    auto nans = presamples;
    auto optr = out->samples;
    for (unsigned i = 0; i < samples->count; ++i) {
        auto val = samples->samples[i];
        if (num == sps) {
            *optr++ = nans == num ? NAN : sum / (num - nans);
            num = 1;
            if (isnan(val)) {
                sum = 0;
                nans = 1;
            } else {
                sum = val;
                nans = 0;
            }
        } else {
            num += 1;
            if (isnan(val)) {
                nans += 1;
            } else {
                sum += val;
            }
        }
    }
    *optr++ = (nans == num) ? NAN : sum / (num - nans);
    assert(optr == out->samples + out->count);
    return out;
}

//===========================================================================
SourceNode::OutputResultReturn SourceNode::outputResult(
    const ResultInfo & info
) {
    scoped_lock lk{m_outMut};
    if (m_outputs.empty())
        return {false, !m_pendingOutputs.empty()};

    auto baseInterval = info.samples
        ? info.samples->interval
        : Duration::max();
    auto maxInterval = baseInterval;
    auto nextMin = Duration::max();
    for (auto && rr : m_outputs) {
        if (rr.minInterval > maxInterval) {
            if (rr.minInterval < nextMin)
                nextMin = rr.minInterval;
            continue;
        }
        rr.rn->onResult(info);
    }

    while (nextMin != Duration::max()) {
        ResultInfo out{info};
        out.samples = consolidateAvg(info.samples, nextMin);
        auto minInterval = maxInterval;
        maxInterval = out.samples->interval;

        nextMin = Duration::max();
        for (auto && rr : m_outputs) {
            if (rr.minInterval > maxInterval) {
                if (rr.minInterval < nextMin)
                    nextMin = rr.minInterval;
                continue;
            }
            if (rr.minInterval < minInterval)
                continue;
            rr.rn->onResult(out);
        }
    }
    if (!info.samples)
        m_outputs.clear();

    return {(bool) info.samples, !m_pendingOutputs.empty()};
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
    if (!outputRange(&m_range))
        return;

    assert(m_unfinished.empty());
    m_result = {};
    m_result.target = sourceName();
    dbFindMetrics(&m_unfinished, s_db, m_result.target.get());
    if (m_unfinished.empty()) {
        if (outputResult(m_result).pending)
            onSourceStart();
        return;
    }
    readMore();
}

//===========================================================================
void DbDataNode::readMore() {
    m_tid = this_thread::get_id();
    for (;;) {
        auto id = m_unfinished.pop_front();
        if (!dbGetSamples(
            this,
            s_db,
            id,
            m_range.first - m_range.pretime,
            m_range.last,
            m_range.presamples
        )) {
            return;
        }
        if (m_unfinished.empty())
            break;
    }
    m_tid = {};
}

//===========================================================================
bool DbDataNode::onDbSeriesStart(const DbSeriesInfo & info) {
    if (!info.type)
        return true;

    auto first = m_range.first
        - m_range.pretime
        - m_range.presamples * info.interval;
    first -= first.time_since_epoch() % info.interval;
    auto last = m_range.last + info.interval;
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
    auto ret = outputResult(m_result);

    m_result.name = {};
    m_result.samples = {};

    if (ret.more && m_unfinished.empty())
        ret = outputResult(m_result);
    if (!ret.more) {
        m_unfinished.clear();
        if (ret.pending)
            onSourceStart();
        return;
    }

    if (m_tid != this_thread::get_id())
        readMore();
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
void FuncNode::init(std::shared_ptr<char[]> sourceName) {
    incRef();
    SourceNode::init(sourceName);
}

//===========================================================================
bool FuncNode::bind(vector<FuncArg> && args) {
    m_args = move(args);
    return onFuncBind();
}

//===========================================================================
void FuncNode::onSourceStart() {
    ResultRange rr;
    if (!outputRange(&rr))
        return;

    onFuncAdjustRange(&rr.first, &rr.last, &rr.pretime, &rr.presamples);
    m_unfinished = (int) m_sources.size();
    rr.rn = this;
    for (auto && sn : m_sources)
        sn->addOutput(rr);
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
            stop = !onFuncApply(info);
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
bool FuncNode::onFuncBind() {
    return true;
}

//===========================================================================
void FuncNode::onFuncAdjustRange(
    TimePoint * first,
    TimePoint * last,
    Duration * pretime,
    unsigned * presamples
)
{}


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
    assert(s_execs.empty());
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void Eval::registerFunc(
    Query::Function::Type type,
    IFactory<FuncNode> * fact
) {
    assert(!s_funcFacts[type]);
    s_funcFacts[type] = fact;
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
    initializeFuncs();
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
    SourceNode::ResultRange rr;
    rr.rn = ex;
    rr.first = first;
    rr.last = last;
    rr.minInterval = ex->m_minInterval;
    auto sn = addSource(ex, target);
    if (!sn) {
        notify->onEvalError("Invalid target parameter: " + string(target));
        return;
    }

    sn->addOutput(rr);
    hostage.release();
}
