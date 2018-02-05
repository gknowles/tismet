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

    virtual void onResult(const ResultInfo & info);
    void onTask() override;

    int m_unfinished{0};
    vector<shared_ptr<SourceNode>> m_sources;

protected:
    virtual Apply onResultTask(ResultInfo & info);

    void stopSources();
    void addResult(const ResultInfo & info);

    mutex m_resMut;
    deque<ResultInfo> m_results;
};

struct ResultRange {
    ResultNode * rn{nullptr};
    TimePoint first;
    TimePoint last;
};

class SourceNode : public ITaskNotify {
public:
    struct OutputRangeResult {
        TimePoint first;
        TimePoint last;
        bool found;
    };

public:
    bool outputRange(TimePoint * first, TimePoint * last) const;

    virtual void onStart();
    void onTask() override;

    shared_ptr<char[]> m_source;
    unordered_map<shared_ptr<char[]>, shared_ptr<SampleList>> m_sampleLists;
    vector<SourceRange> m_ranges;

    mutable mutex m_outMut;
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
    void onResult(const ResultInfo & info) override;
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
    void onResult(const ResultInfo & info) override;
};

class FuncNonNegativeDerivative : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncScale : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class FuncSum : public FuncNode {
    Apply onResultTask(ResultInfo & info) override;
    ResultInfo m_out;
};

class FuncTimeShift : public FuncNode {
    Apply onFuncApply(ResultInfo & info) override;
};

class Evaluate : public ResultNode, public ListBaseLink<> {
public:
    Apply onResultTask(ResultInfo & info) override;

    IEvalNotify * m_notify{nullptr};
    TimePoint m_first;
    TimePoint m_last;
    int m_maxPoints{0};
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
    auto si = lower_bound(rn->m_sources.begin(), rn->m_sources.end(), src);
    rn->m_sources.insert(si, src);
    return src;
}

//===========================================================================
static shared_ptr<SourceNode> addSource(ResultNode * rn, string_view srcv) {
    if (auto si = s_sources.find(srcv); si != s_sources.end())
        return addSource(rn, si->second);

    auto src = toSharedString(srcv);
    Query::QueryInfo qi;
    if (!Query::parse(qi, src.get()))
        return {};

    auto type = Query::getType(*qi.node);
    if (type == Query::kPath) {
        auto sn = make_shared<SourceNode>();
        sn->m_source = src;
        s_sources[src.get()] = sn;
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
    fnode->m_unfinished = (int) qf.args.size();
    s_sources[src.get()] = fnode;
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


/****************************************************************************
*
*   SourceNode
*
***/

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
void SourceNode::onStart() {
    taskPushCompute(this);
}

//===========================================================================
void SourceNode::onTask() {
    TimePoint first, last;
    if (!outputRange(&first, &last))
        return;

    UnsignedSet ids;
    dbFindMetrics(ids, s_db, m_source.get());
    ResultInfo info;
    info.target = m_source;
    if (ids.empty()) {
        scoped_lock<mutex> lk{m_outMut};
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

        scoped_lock<mutex> lk{m_outMut};
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
    stopSources();
}

//===========================================================================
void ResultNode::stopSources() {
    for (auto && sn : m_sources)
        removeOutput(sn.get(), this);
}

//===========================================================================
void ResultNode::addResult(const ResultInfo & info) {
    scoped_lock<mutex> lk{m_resMut};
    m_results.push_back(info);
    if (m_results.size() == 1)
        taskPushCompute(this);
}

//===========================================================================
void ResultNode::onResult(const ResultInfo & info) {
    addResult(info);
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
*   FuncNode
*
***/

//===========================================================================
FuncNode::~FuncNode() {
    assert(m_outputs.empty());
}

//===========================================================================
void FuncNode::forwardResult(ResultInfo & info, bool unfinished) {
    scoped_lock<mutex> lk{m_outMut};
    if (unfinished)
        info.more = (info.more || --m_unfinished);
    if (info.samples || !info.more) {
        for (auto && rr : m_outputs)
            rr.rn->onResult(info);
        if (!info.more)
            m_outputs.clear();
    }
}

//===========================================================================
void FuncNode::onStart() {
    TimePoint first, last;
    if (!outputRange(&first, &last))
        return;

    m_unfinished = (int) m_sources.size();
    for (auto && sn : m_sources)
        addOutput(sn, this, first, last);
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
void FuncAlias::onResult(const ResultInfo & info) {
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
void FuncMaximumAbove::onResult(const ResultInfo & info) {
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
    for (int i = info.samples->count; i-- > 0;) {
        best = info.samples->samples[i];
        if (!isnan(best))
            break;
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
*   FuncSum
*
***/

//===========================================================================
FuncNode::Apply FuncSum::onResultTask(ResultInfo & info) {
    if (info.samples) {
        if (!m_out.samples) {
            m_out.samples = copySampleList(*info.samples);
        } else if (m_out.samples->first == info.samples->first
            && m_out.samples->count == info.samples->count
            && m_out.samples->interval == info.samples->interval
        ) {
            auto optr = m_out.samples->samples;
            auto ptr = info.samples->samples;
            auto eptr = ptr + info.samples->count;
            for (; ptr != eptr; ++ptr, ++optr)
                *optr += *ptr;
        } else {
            // TODO: normalize and consolidate incompatible lists
            logMsgError() << "summing incompatible series";
        }
    }

    if (!info.more) {
        m_out.target = info.target;
        m_out.name = addFuncName(m_type, info.target);
        m_out.more = false;
        forwardResult(m_out);
        m_out.samples.reset();
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   Evaluate
*
***/

//===========================================================================
FuncNode::Apply Evaluate::onResultTask(ResultInfo & info) {
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
            m_notify->onEvalEnd();
            return Apply::kDestroy;
        }
        auto samp = info.samples->samples;
        for (; first < last; first += info.samples->interval, ++samp) {
            if (!m_notify->onDbSample(first, *samp)) {
                m_notify->onEvalEnd();
                return Apply::kDestroy;
            }
        }
        m_notify->onDbSeriesEnd(info.samples->metricId);
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
        if (auto sn = addSource(ex, target)) {
            addOutput(sn, ex, first, last);
        } else {
            delete ex;
            notify->onEvalError("Invalid target parameter: " + string(target));
            return;
        }
    }
}
