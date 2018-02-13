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

class Evaluate : public ResultNode, public ListBaseLink<> {
public:
    Evaluate();
    ~Evaluate();

    void onResult(int resultId, const ResultInfo & info) override;
    Apply onResultTask(ResultInfo & info) override;

    IEvalNotify * m_notify{nullptr};
    DbContextHandle m_ctx;
    TimePoint m_first;
    TimePoint m_last;
    Duration m_minInterval;

    int m_curId{0};
    vector<deque<ResultInfo>> m_idResults;
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
static void addOutput(
    shared_ptr<SourceNode> src,
    ResultNode * rn,
    int resultId,
    TimePoint first,
    TimePoint last,
    Duration minInterval
) {
    unique_lock<mutex> lk{src->m_outMut};
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
        info.name = {};
        info.samples = {};
        info.more = false;
        rn->onResult(resultId, info);
        return;
    }

    src->m_outputs.push_back({rn, resultId, first, last, minInterval});
    if (src->m_outputs.size() != 1)
        return;

    lk.unlock();
    src->onStart();
}

//===========================================================================
static void removeOutput(SourceNode * src, ResultNode * rn) {
    scoped_lock<mutex> lk{src->m_outMut};
    auto ptr = src->m_outputs.data(),
        eptr = ptr + src->m_outputs.size();
    while (ptr != eptr) {
        if (ptr->rn == rn) {
            if (--eptr == ptr)
                break;
            *ptr = *eptr;
        } else {
            ptr += 1;
        }
    }
    src->m_outputs.resize(eptr - src->m_outputs.data());
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
    shared_lock<shared_mutex> lk{s_mut};
    if (auto si = s_sources.find(srcv); si != s_sources.end())
        return addSource(rn, si->second);
    lk.unlock();

    auto src = toSharedString(srcv);
    Query::QueryInfo qi;
    if (!Query::parse(qi, src.get()))
        return {};

    auto type = Query::getType(*qi.node);
    if (type == Query::kPath) {
        auto sn = make_shared<SourceNode>();
        sn->m_source = src;
        {
            scoped_lock<shared_mutex> lk{s_mut};
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
        fnode = fact->onFactoryCreate();
    } else {
        assert(!"Unsupported function");
        return {};
    }
    fnode->m_source = src;
    fnode->m_type = qf.type;
    {
        scoped_lock<shared_mutex> lk{s_mut};
        s_sources[src.get()] = fnode;
    }
    ResultInfo info;
    info.target = src;
    for (auto && arg : qf.args) {
        switch (Query::getType(*arg)) {
        case Query::kPath:
        case Query::kFunc:
            if (!addSource(fnode.get(), toString(*arg)))
                return {};
            break;
        case Query::kNum:
            fnode->m_args.emplace_back().number = Query::getNumber(*arg);
            break;
        case Query::kString:
            fnode->m_args.emplace_back().string =
                toSharedString(Query::getString(*arg));
            break;
        default:
            return {};
        }
    }
    if (!fnode->onFuncBind())
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
SourceNode::SourceNode()
    : m_reader(this)
{}

//===========================================================================
bool SourceNode::outputRange(TimePoint * first, TimePoint * last) const {
    *first = TimePoint::max();
    *last = TimePoint::min();

    scoped_lock<mutex> lk{m_outMut};
    if (m_outputs.empty())
        return false;
    for (auto && rr : m_outputs) {
        if (*first > rr.first)
            *first = rr.first;
        if (*last < rr.last)
            *last = rr.last;
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
    auto first = samples->first + maxInterval - baseInterval;
    first -= first.time_since_epoch() % maxInterval;
    auto skip = (first - samples->first) / baseInterval;
    auto count = samples->count - skip;
    auto out = SampleList::alloc(
        first,
        maxInterval,
        (count + sps - 1) / sps
    );

    auto sum = samples->samples[skip];
    auto num = 1;
    auto nans = isnan(sum) ? 1 : 0;
    auto optr = out->samples;
    for (auto i = skip + 1; i < samples->count; ++i) {
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
bool SourceNode::outputResult(const ResultInfo & info) {
    scoped_lock<mutex> lk{m_outMut};
    if (m_outputs.empty())
        return info.more;

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
        rr.rn->onResult(rr.resultId, info);
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
            rr.rn->onResult(rr.resultId, out);
        }
    }

    if (!info.more)
        m_outputs.clear();
    return info.more;
}

//===========================================================================
void SourceNode::onStart() {
    taskPushCompute(this);
}

//===========================================================================
void SourceNode::onTask() {
    TimePoint first, last;
    if (!outputRange(&first, &last))
        return;

    m_reader.read(m_source, first, last);
}


/****************************************************************************
*
*   SampleReader
*
***/

//===========================================================================
SampleReader::SampleReader(SourceNode * node)
    : m_node(*node)
{}

//===========================================================================
void SampleReader::read(
    shared_ptr<char[]> target,
    TimePoint first,
    TimePoint last
) {
    assert(m_unfinished.empty());
    m_result = {};
    m_result.target = target;
    m_first = first;
    m_last = last;
    dbFindMetrics(&m_unfinished, s_db, target.get());
    if (m_unfinished.empty()) {
        m_node.outputResult(m_result);
        return;
    }
    readMore();
}

//===========================================================================
void SampleReader::readMore() {
    m_tid = this_thread::get_id();
    for (;;) {
        auto id = m_unfinished.pop_front();
        if (!dbGetSamples(this, s_db, id, m_first, m_last))
            return;
        if (m_unfinished.empty())
            break;
    }
    m_tid = {};
}

//===========================================================================
bool SampleReader::onDbSeriesStart(const DbSeriesInfo & info) {
    if (!info.type)
        return true;
    auto count = (info.last - info.first) / info.interval;
    if (!count)
        return true;
    m_result.name = toSharedString(info.name);
    m_result.samples = SampleList::alloc(info.first, info.interval, count);
    m_result.samples->metricId = info.id;
    m_pos = 0;
    m_time = info.first;
    return true;
}

//===========================================================================
bool SampleReader::onDbSample(uint32_t id, TimePoint time, double value) {
    auto samples = m_result.samples->samples;
    auto interval = m_result.samples->interval;
    for (; m_time < time; m_time += interval, ++m_pos)
        samples[m_pos] = NAN;
    samples[m_pos] = value;
    m_time += interval;
    m_pos += 1;
    return true;
}

//===========================================================================
void SampleReader::onDbSeriesEnd(uint32_t id) {
    if (m_result.samples) {
        auto samples = m_result.samples->samples;
        auto interval = m_result.samples->interval;
        auto count = m_result.samples->count;
        auto last = m_result.samples->first + count * interval;
        for (; m_time < last; m_time += interval, ++m_pos)
            samples[m_pos] = NAN;
        assert(m_pos == count);
    }

    m_result.more = !m_unfinished.empty();
    auto more = m_node.outputResult(m_result);
    m_result.name = {};
    m_result.samples = {};
    if (!more) {
        m_unfinished.clear();
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
        removeOutput(sn.get(), this);
}

//===========================================================================
void ResultNode::onResult(int resultId, const ResultInfo & info) {
    scoped_lock<mutex> lk{m_resMut};
    m_results.push_back(info);
    if (m_results.size() == 1)
        taskPushCompute(this);
}

//===========================================================================
void ResultNode::onTask() {
    unique_lock<mutex> lk{m_resMut};
    assert(!m_results.empty());
    auto mode = Apply::kSkip;
    for (;;) {
        auto info = m_results.front();
        lk.unlock();
        info.more = info.more || --m_unfinished;
        if (!info.more || info.samples) {
            mode = onResultTask(info);
            switch (mode) {
            case Apply::kForward:
                logMsgCrash() << "onResultTask returned Apply::kForward";
                return;
            case Apply::kSkip:
                break;
            case Apply::kFinished:
                stopSources();
                break;
            case Apply::kDestroy:
                delete this;
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
ResultNode::Apply ResultNode::onResultTask(ResultInfo & info) {
    assert(!"onResultTask not implemented");
    return Apply::kDestroy;
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
void FuncNode::forwardResult(ResultInfo & info, bool unfinished) {
    if (unfinished)
        info.more = (info.more || --m_unfinished);
    if (info.samples || !info.more)
        outputResult(info);
}

//===========================================================================
void FuncNode::onStart() {
    TimePoint first, last;
    if (!outputRange(&first, &last))
        return;

    m_unfinished = (int) m_sources.size();
    int id = 0;
    for (auto && sn : m_sources)
        addOutput(sn, this, id++, first, last, {});
}

//===========================================================================
FuncNode::Apply FuncNode::onResultTask(ResultInfo & info) {
    auto mode = Apply::kForward;
    if (info.samples)
        mode = onFuncApply(info);
    if (mode == Apply::kForward) {
        forwardResult(info);
        return Apply::kSkip;
    }
    return mode;
}

//===========================================================================
bool FuncNode::onFuncBind() {
    return true;
}

//===========================================================================
FuncNode::Apply FuncNode::onFuncApply(ResultInfo & info) {
    assert(!"onFuncApply not implemented");
    return Apply::kDestroy;
}


/****************************************************************************
*
*   Evaluate
*
***/

//===========================================================================
Evaluate::Evaluate() {
    unique_lock<shared_mutex> lk{s_mut};
    s_execs.link(this);
}

//===========================================================================
Evaluate::~Evaluate() {
    dbCloseContext(m_ctx);

    unique_lock<shared_mutex> lk{s_mut};
    s_execs.unlink(this);
}

//===========================================================================
void Evaluate::onResult(int resultId, const ResultInfo & info) {
    scoped_lock<mutex> lk{m_resMut};
    if (resultId != m_curId) {
        assert(resultId > m_curId);
        m_idResults[resultId].push_back(info);
        return;
    }

    m_results.push_back(info);
    if (m_results.size() == 1)
        taskPushCompute(this);
    auto more = info.more;
    while (!more) {
        if (++m_curId == m_idResults.size())
            break;
        if (m_idResults[m_curId].empty()) {
            more = true;
        } else {
            for (auto && ri : m_idResults[m_curId])
                m_results.push_back(move(ri));
            more = m_results.back().more;
        }
    }
}

//===========================================================================
FuncNode::Apply Evaluate::onResultTask(ResultInfo & info) {
    if (info.samples) {
        auto first = info.samples->first;
        auto last = first + info.samples->count * info.samples->interval;
        DbSeriesInfo dsi;
        dsi.target = info.target.get();
        dsi.id = info.samples->metricId;
        dsi.name = info.name.get();
        dsi.type = kSampleTypeFloat64;
        dsi.first = first;
        dsi.last = last;
        dsi.interval = info.samples->interval;
        if (!m_notify->onDbSeriesStart(dsi)) {
            m_notify->onEvalEnd();
            return Apply::kDestroy;
        }
        auto samp = info.samples->samples;
        for (; first < last; first += dsi.interval, ++samp) {
            if (!m_notify->onDbSample(dsi.id, first, *samp)) {
                m_notify->onEvalEnd();
                return Apply::kDestroy;
            }
        }
        m_notify->onDbSeriesEnd(dsi.id);
    }
    if (info.more)
        return Apply::kSkip;

    m_notify->onEvalEnd();
    return Apply::kDestroy;
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
    shared_lock<shared_mutex> lk{s_mut};
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
void evalAdd(
    IEvalNotify * notify,
    const vector<string_view> & targets,
    TimePoint first,
    TimePoint last,
    size_t maxPoints
) {
    auto ex = new Evaluate;
    ex->m_notify = notify;
    ex->m_ctx = dbOpenContext(s_db);
    ex->m_unfinished = (int) targets.size();
    ex->m_first = first;
    ex->m_last = last;
    if (maxPoints)
        ex->m_minInterval = (last - first) / maxPoints;
    int id = 0;
    ex->m_idResults.resize(targets.size());
    for (auto && target : targets) {
        if (auto sn = addSource(ex, target)) {
            addOutput(sn, ex, id++, first, last, ex->m_minInterval);
        } else {
            delete ex;
            notify->onEvalError("Invalid target parameter: " + string(target));
            return;
        }
    }
}
