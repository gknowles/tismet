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

class ResultNode : public ITaskNotify {
public:
    enum class Apply {
        kForward,
        kSkip,
        kFinished,
        kDestroy
    };

public:
    virtual ~ResultNode();

    virtual void onResult(int resultId, const ResultInfo & info);
    void onTask() override;

    int m_unfinished{0};
    vector<shared_ptr<SourceNode>> m_sources;

protected:
    virtual Apply onResultTask(ResultInfo & info);

    void stopSources();

    mutex m_resMut;
    deque<ResultInfo> m_results;
};

struct ResultRange {
    ResultNode * rn{nullptr};
    int resultId{0};
    TimePoint first;
    TimePoint last;
    Duration minInterval;
};

class SampleReader : public IDbDataNotify {
public:
    SampleReader(SourceNode * node);
    void read(shared_ptr<char[]> target, TimePoint first, TimePoint last);

private:
    void readMore();

    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(uint32_t id, TimePoint time, double value) override;
    void onDbSeriesEnd(uint32_t id) override;

    SourceNode & m_node;
    thread::id m_tid;
    ResultInfo m_result;
    TimePoint m_first;
    TimePoint m_last;
    UnsignedSet m_unfinished;

    size_t m_pos{0};
    TimePoint m_time;
};

class SourceNode : public ITaskNotify {
public:
    struct OutputRangeResult {
        TimePoint first;
        TimePoint last;
        bool found;
    };

public:
    SourceNode();

    bool outputRange(TimePoint * first, TimePoint * last) const;

    // Returns false when !info.more
    bool outputResult(const ResultInfo & info);

    virtual void onStart();
    void onTask() override;

    shared_ptr<char[]> m_source;
    vector<SourceRange> m_ranges;

    mutable mutex m_outMut;
    vector<ResultRange> m_outputs;

private:
    SampleReader m_reader;
};

struct FuncArg {
    shared_ptr<char[]> string;
    double number;
};

class FuncNode : public ResultNode, public SourceNode {
public:
    ~FuncNode ();

    void forwardResult(ResultInfo & info, bool unfinished = false);

    void onStart() override;
    Apply onResultTask(ResultInfo & info) override;
    virtual Apply onFuncApply(ResultInfo & info);

    Query::Function::Type m_type;
    vector<FuncArg> m_args;
};

class FuncAlias : public FuncNode {
    void onResult(int resultId, const ResultInfo & info) override;
};

class FuncDerivative : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncHighestCurrent : public FuncNode {
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
class FuncHighestMax : public FuncNode {
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};

class FuncKeepLastValue : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncMaximumAbove : public FuncNode {
    void onResult(int resultId, const ResultInfo & info) override;
};

class FuncNonNegativeDerivative : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncScale : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncSum : public FuncNode {
    Apply onResultTask(ResultInfo & info) override;
    shared_ptr<SampleList> m_samples;
};

class FuncTimeShift : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

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
    switch (qf.type) {
    case Query::Function::kAlias:
        fnode = make_shared<FuncAlias>();
        break;
    case Query::Function::kDerivative:
        fnode = make_shared<FuncDerivative>();
        break;
    case Query::Function::kHighestCurrent:
        fnode = make_shared<FuncHighestCurrent>();
        break;
    case Query::Function::kHighestMax:
        fnode = make_shared<FuncHighestMax>();
        break;
    case Query::Function::kKeepLastValue:
        fnode = make_shared<FuncKeepLastValue>();
        break;
    case Query::Function::kMaximumAbove:
        fnode = make_shared<FuncMaximumAbove>();
        break;
    case Query::Function::kNonNegativeDerivative:
        fnode = make_shared<FuncNonNegativeDerivative>();
        break;
    case Query::Function::kScale:
        fnode = make_shared<FuncScale>();
        break;
    case Query::Function::kSum:
        fnode = make_shared<FuncSum>();
        break;
    case Query::Function::kTimeShift:
        fnode = make_shared<FuncTimeShift>();
        break;
    default:
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
    return addSource(rn, fnode);
}

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
static shared_ptr<SampleList> allocSampleList(const SampleList & samples) {
    return allocSampleList(samples.first, samples.interval, samples.count);
}

//===========================================================================
static shared_ptr<SampleList> copySampleList(const SampleList & samples) {
    auto out = allocSampleList(samples);
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
    auto out = allocSampleList(
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
    auto count = (info.last - info.first) / info.interval;
    if (!count)
        return false;
    m_result.name = toSharedString(info.name);
    m_result.samples = allocSampleList(info.first, info.interval, count);
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
*   Function helpers
*
***/

//===========================================================================
static shared_ptr<char[]> addFuncName(
    Query::Function::Type ftype,
    const shared_ptr<char[]> & prev
) {
    auto fname = string_view(Query::getFuncName(ftype, "UNKNOWN"));
    auto prevLen = strlen(prev.get());
    auto newLen = prevLen + fname.size() + 2;
    auto out = shared_ptr<char[]>(new char[newLen + 1]);
    auto ptr = out.get();
    memcpy(ptr, fname.data(), fname.size());
    ptr += fname.size();
    *ptr++ = '(';
    memcpy(ptr, prev.get(), prevLen);
    if (newLen <= 1000) {
        ptr += prevLen;
        *ptr++ = ')';
        *ptr = 0;
    } else {
        out[996] = out[997] = out[998] = '.';
        out[999] = 0;
    }
    return out;
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
FuncNode::Apply FuncNode::onFuncApply(ResultInfo & info) {
    assert(!"onFuncApply not implemented");
    return Apply::kDestroy;
}


/****************************************************************************
*
*   FuncAlias
*
***/

//===========================================================================
void FuncAlias::onResult(int resultId, const ResultInfo & info) {
    ResultInfo out{info};
    if (out.samples)
        out.name = m_args[0].string;
    forwardResult(out, true);
}


/****************************************************************************
*
*   FuncMaximumAbove
*
***/

//===========================================================================
void FuncMaximumAbove::onResult(int resultId, const ResultInfo & info) {
    ResultInfo out{info};
    if (out.samples) {
        auto limit = m_args[0].number;
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        if (find_if(ptr, eptr, [&](auto val){ return val > limit; }) == eptr) {
            out.name = {};
            out.samples = {};
        }
    }
    forwardResult(out, true);
}


/****************************************************************************
*
*   FuncKeepLastValue
*
***/

//===========================================================================
FuncNode::Apply FuncKeepLastValue::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto limit = m_args.empty() ? 0 : (int) m_args[0].number;
    auto base = info.samples->samples;
    auto eptr = base + info.samples->count;
    int nans = 0;
    for (; base != eptr; ++base) {
        if (!isnan(*base))
            break;
    }
    for (auto ptr = base; ptr != eptr; ++ptr) {
        if (isnan(*ptr)) {
            if (!nans++)
                base = ptr - 1;
        } else if (nans) {
            if (!limit || nans <= limit) {
                auto val = *base++;
                for (; base != ptr; ++base)
                    *base = val;
            }
            nans = 0;
        }
    }
    if (nans && (!limit || nans <= limit)) {
        auto val = *base++;
        for (; base != eptr; ++base)
            *base = val;
    }
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncDerivative
*
***/

//===========================================================================
FuncNode::Apply FuncDerivative::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto out = allocSampleList(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count - 1;
    *optr++ = NAN;
    for (; ptr != eptr; ++ptr) {
        *optr++ = ptr[1] - ptr[0];
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncNonNegativeDerivative
*
***/

//===========================================================================
FuncNode::Apply FuncNonNegativeDerivative::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto limit = m_args.empty() ? HUGE_VAL : m_args[0].number;

    auto out = allocSampleList(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    *optr++ = NAN;
    auto prev = (double) NAN;
    for (ptr += 1; ptr != eptr; ++ptr) {
        if (*ptr > limit) {
            prev = *optr++ = NAN;
        } else if (*ptr >= prev) {
            auto next = *ptr;
            *optr++ = *ptr - prev;
            prev = next;
        } else {
            auto next = *ptr;
            *optr++ = *ptr + (limit - prev + 1);
            prev = next;
        }
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncScale
*
***/

//===========================================================================
FuncNode::Apply FuncScale::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto factor = m_args[0].number;

    auto out = allocSampleList(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    for (; ptr != eptr; ++ptr) {
        *optr++ = *ptr * factor;
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncTimeShift
*
***/

//===========================================================================
FuncNode::Apply FuncTimeShift::onFuncApply(ResultInfo & info) {
    auto tmp = string(m_args[0].string.get());
    if (tmp[0] != '+' && tmp[0] != '-')
        tmp = "-" + tmp;
    Duration shift;
    if (!parse(&shift, tmp.c_str()))
        return Apply::kFinished;

    info.name = addFuncName(m_type, info.name);
    info.samples = copySampleList(*info.samples);
    info.samples->first += shift;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncHighestCurrent
*
***/

//===========================================================================
FuncNode::Apply FuncHighestCurrent::onResultTask(ResultInfo & info) {
    auto allowed = m_args[0].number;

    // last non-NAN sample in list
    auto best = (double) NAN;
    if (info.samples) {
        for (int i = info.samples->count; i-- > 0;) {
            best = info.samples->samples[i];
            if (!isnan(best))
                break;
        }
    }

    if (!isnan(best)) {
        if (m_best.size() < allowed) {
            m_best.emplace(best, info);
        } else if (auto i = m_best.begin();  i->first < best) {
            m_best.erase(i);
            m_best.emplace(best, info);
        }
    }
    if (!info.more) {
        for (auto && out : m_best) {
            out.second.more = true;
            forwardResult(out.second);
        }
        ResultInfo out{};
        out.target = info.target;
        forwardResult(out);
        m_best.clear();
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   FuncHighestMax
*
***/

//===========================================================================
FuncNode::Apply FuncHighestMax::onResultTask(ResultInfo & info) {
    auto allowed = m_args[0].number;

    // largest non-NAN sample in list
    auto best = -numeric_limits<double>::infinity();
    bool found = false;
    if (info.samples) {
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        for (; ptr != eptr; ++ptr) {
            if (*ptr > best) {
                best = *ptr;
                found = true;
            }
        }
    }

    if (found) {
        if (m_best.size() < allowed) {
            m_best.emplace(best, info);
        } else if (auto i = m_best.begin();  best > i->first) {
            m_best.erase(i);
            m_best.emplace(best, info);
        }
    }
    if (!info.more) {
        for (auto i = m_best.rbegin(), ei = m_best.rend(); i != ei; ++i) {
            i->second.more = true;
            forwardResult(i->second);
        }
        ResultInfo out{};
        out.target = info.target;
        forwardResult(out);
        m_best.clear();
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   FuncSum
*
***/

//===========================================================================
FuncNode::Apply FuncSum::onResultTask(ResultInfo & info) {
    if (info.samples) {
        if (!m_samples) {
            m_samples = copySampleList(*info.samples);
        } else if (m_samples->interval == info.samples->interval) {
            auto resize = false;
            auto interval = m_samples->interval;
            auto slast = m_samples->first + m_samples->count * interval;
            auto ilast = info.samples->first + info.samples->count * interval;
            auto first = m_samples->first;
            auto last = slast;
            if (info.samples->first < first) {
                first = info.samples->first;
                resize = true;
            }
            if (ilast > slast) {
                last = ilast;
                resize = true;
            }
            if (resize) {
                auto tmp = allocSampleList(
                    first,
                    interval,
                    (last - first) / interval
                );
                auto pos = 0;
                for (; first < m_samples->first; first += interval, ++pos)
                    tmp->samples[pos] = NAN;
                auto spos = 0;
                for (; first < slast; first += interval, ++pos, ++spos)
                    tmp->samples[pos] = m_samples->samples[spos];
                for (; first < last; first += interval, ++pos)
                    tmp->samples[pos] = NAN;
                assert(pos == (int) tmp->count);
                m_samples = tmp;
                slast = first;
                first = m_samples->first;
            }
            auto ipos = 0;
            auto pos = (info.samples->first - first) / interval;
            first = info.samples->first;
            for (; first < ilast; first += interval, ++pos, ++ipos) {
                auto & sval = m_samples->samples[pos];
                auto & ival = info.samples->samples[ipos];
                if (isnan(sval)) {
                    sval = ival;
                } else if (!isnan(ival)) {
                    sval += ival;
                }
            }
            assert(pos <= m_samples->count);
        } else {
            // TODO: normalize and consolidate incompatible lists
            logMsgError() << "summing incompatible series";
        }
    }

    if (!info.more) {
        ResultInfo out;
        out.target = info.target;
        out.name = addFuncName(m_type, info.target);
        out.samples = move(m_samples);
        out.more = false;
        forwardResult(out);
    }
    return Apply::kSkip;
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
*   Public API
*
***/

//===========================================================================
void evalInitialize(DbHandle f) {
    shutdownMonitor(&s_cleanup);
    s_db = f;
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
