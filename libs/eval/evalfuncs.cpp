// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// evalfuncs.cpp - tismet eval
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;
using namespace Query;


/****************************************************************************
*
*   Private
*
***/

namespace {

template<typename T>
class Register {
public:
    Register(Function::Type ftype) {
        registerFunc(ftype, getFactory<FuncNode, T>());
    }
};

template<Function::Type FT, typename T>
class FuncImpl : public FuncNode {
public:
    inline static Register<T> s_register{FT};
public:
    Query::Function::Type type() const override { return FT; }
    void onFuncAdjustRange(
        TimePoint * first,
        TimePoint * last,
        Duration * pretime,
        unsigned * presamples
    ) override;

protected:
    Duration m_pretime{};
    unsigned m_presamples{0};

private:
    Duration m_oldPretime{};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static shared_ptr<char[]> addFuncName(
    Function::Type ftype,
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
*   FuncImpl
*
***/

//===========================================================================
template<Function::Type FT, typename T>
void FuncImpl<FT, T>::onFuncAdjustRange(
    TimePoint * first,
    TimePoint * last,
    Duration * pretime,
    unsigned * presamples
) {
    m_oldPretime = *pretime;
    *pretime += m_pretime;
    *presamples += m_presamples;
}


/****************************************************************************
*
*   FuncAlias
*
***/

namespace {
class FuncAlias : public FuncImpl<Function::kAlias, FuncAlias> {
    void onResult(int resultId, const ResultInfo & info) override;
};
} // namespace

//===========================================================================
void FuncAlias::onResult(int resultId, const ResultInfo & info) {
    ResultInfo out{info};
    if (out.samples)
        out.name = m_args[0].string;
    forwardResult(out, true);
}


/****************************************************************************
*
*   Filter - exclude sample lists from results
*
***/

//===========================================================================
// maximumAbove
//===========================================================================
namespace {
class FuncMaximumAbove
    : public FuncImpl<Function::kMaximumAbove, FuncMaximumAbove>
{
    void onResult(int resultId, const ResultInfo & info) override;
};
} // namespace

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
*   Transform - make changes within a single SampleList
*
***/

namespace {
template<Function::Type FT, typename T>
class Transform : public FuncImpl<FT, T> {
    FuncNode::Apply onFuncApply(ResultInfo & info) override;

    virtual void onTransformStart(Duration interval) {}
    virtual void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) = 0;
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
FuncNode::Apply Transform<FT, T>::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(this->type(), info.name);

    auto out = SampleList::alloc(*info.samples);
    onTransformStart(out->interval);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    onTransform(optr, ptr, eptr);
    info.samples = out;
    return FuncNode::Apply::kForward;
}

//===========================================================================
// derivative
//===========================================================================
namespace {
class FuncDerivative
    : public Transform<Function::kDerivative, FuncDerivative>
{
    bool onFuncBind() override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;
};
} // namespace

//===========================================================================
bool FuncDerivative::onFuncBind() {
    m_presamples = 1;
    return true;
}

//===========================================================================
void FuncDerivative::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr
) {
    *optr++ = NAN;
    for (; ptr != eptr - 1; ++ptr) {
        *optr++ = ptr[1] - ptr[0];
    }
}

//===========================================================================
// keepLastValue
//===========================================================================
namespace {
class FuncKeepLastValue
    : public Transform<Function::kKeepLastValue, FuncKeepLastValue>
{
    bool onFuncBind() override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;

    double m_limit;
};
} // namespace

//===========================================================================
bool FuncKeepLastValue::onFuncBind() {
    m_limit = m_args.empty() ? 0 : (int) m_args[0].number;
    m_presamples = 1;
    return true;
}

//===========================================================================
void FuncKeepLastValue::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr
) {
    auto base = ptr;
    int nans = 0;
    for (; base != eptr; ++base) {
        if (!isnan(*base))
            break;
        *optr++ = *base;
    }
    for (ptr = base; ptr != eptr; ++ptr) {
        if (isnan(*ptr)) {
            if (!nans++) {
                for (; base != ptr - 1; ++base)
                    *optr++ = *base;
            }
        } else if (nans) {
            if (!m_limit || nans <= m_limit) {
                auto val = *base;
                for (; base != ptr; ++base)
                    *optr++ = val;
            }
            nans = 0;
        }
    }
    if (nans && (!m_limit || nans <= m_limit)) {
        auto val = *base;
        for (; base != ptr; ++base)
            *optr++ = val;
    } else {
        for (; base != ptr; ++base)
            *optr++ = *base;
    }
}

//===========================================================================
// movingAverage
//===========================================================================
namespace {
class FuncMovingAverage
    : public Transform<
        Function::kMovingAverage,
        FuncMovingAverage>
{
    bool onFuncBind() override;
    void onTransformStart(Duration interval) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;

    double m_points{};
    Duration m_interval{};
    int m_count{};
};
} // namespace

//===========================================================================
bool FuncMovingAverage::onFuncBind() {
    if (auto arg0 = m_args[0].string.get()) {
        if (parse(&m_interval, arg0)) {
            m_points = NAN;
        } else {
            m_points = m_count = strToInt(arg0);
            if (!m_count)
                m_count = 1;
        }
    } else {
        m_points = m_count = (int) m_args[0].number;
        if (!m_count)
            m_count = 1;
    }
    return true;
}

//===========================================================================
void FuncMovingAverage::onTransformStart(Duration interval) {
    if (isnan(m_points)) {
        m_count = int(m_interval / interval);
        if (!m_count)
            m_count = 1;
    }
}

//===========================================================================
void FuncMovingAverage::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr
) {
    auto prev = *optr++ = NAN;
    for (ptr += 1; ptr != eptr - 1; ++ptr) {
        if (isnan(*ptr) || *ptr > m_count) {
            prev = *optr++ = NAN;
        } else if (*ptr >= prev) {
            auto next = *ptr;
            *optr++ = *ptr - prev;
            prev = next;
        } else {
            auto next = *ptr;
            *optr++ = *ptr + (m_count - prev + 1);
            prev = next;
        }
    }
}

//===========================================================================
// nonNegativeDerivative
//===========================================================================
namespace {
class FuncNonNegativeDerivative
    : public Transform<
        Function::kNonNegativeDerivative,
        FuncNonNegativeDerivative>
{
    bool onFuncBind() override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;

    double m_limit;
};
} // namespace

//===========================================================================
bool FuncNonNegativeDerivative::onFuncBind() {
    m_limit = m_args.empty() ? HUGE_VAL : m_args[0].number;
    m_presamples = 1;
    return true;
}

//===========================================================================
void FuncNonNegativeDerivative::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr
) {
    *optr++ = NAN;
    auto prev = *ptr;
    for (ptr += 1; ptr != eptr; ++ptr) {
        if (isnan(*ptr) || isnan(prev) || *ptr > m_limit) {
            *optr++ = NAN;
            prev = *ptr;
        } else if (*ptr >= prev) {
            *optr++ = *ptr - prev;
            prev = *ptr;
        } else if (isinf(m_limit)) {
            *optr++ = NAN;
            prev = *ptr;
        } else {
            *optr++ = *ptr + (m_limit - prev + 1);
            prev = *ptr;
        }
    }
}


/****************************************************************************
*
*   Convert - changes to individual samples
*
***/

namespace {
template<Function::Type FT, typename T>
class Convert : public FuncImpl<FT, T> {
    FuncNode::Apply onFuncApply(ResultInfo & info) override;

    virtual double onConvert(double value) = 0;
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
FuncNode::Apply Convert<FT, T>::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(this->type(), info.name);

    auto out = SampleList::alloc(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    for (; ptr != eptr; ++ptr)
        *optr++ = onConvert(*ptr);
    assert(optr == out->samples + out->count);
    info.samples = out;
    return FuncNode::Apply::kForward;
}

//===========================================================================
// drawAsInfinite
//===========================================================================
namespace {
class FuncDrawAsInfinite
    : public Convert<Function::kDrawAsInfinite, FuncDrawAsInfinite>
{
    double onConvert(double value) override;
};
} // namespace

//===========================================================================
double FuncDrawAsInfinite::onConvert(double value) {
    if (value == 0) {
        return 0;
    } else if (value > 0) {
        return HUGE_VAL;
    } else {
        return NAN;
    }
}

//===========================================================================
// removeAboveValue
//===========================================================================
namespace {
class FuncRemoveAboveValue
    : public Convert<Function::kRemoveAboveValue, FuncRemoveAboveValue>
{
    double onConvert(double value) override;
};
} // namespace

//===========================================================================
double FuncRemoveAboveValue::onConvert(double value) {
    auto limit = m_args[0].number;
    return value > limit ? NAN : value;
}

//===========================================================================
// removeBelowValue
//===========================================================================
namespace {
class FuncRemoveBelowValue
    : public Convert<Function::kRemoveBelowValue, FuncRemoveBelowValue>
{
    double onConvert(double value) override;
};
} // namespace

//===========================================================================
double FuncRemoveBelowValue::onConvert(double value) {
    auto limit = m_args[0].number;
    return value < limit ? NAN : value;
}

//===========================================================================
// scale
//===========================================================================
namespace {
class FuncScale : public Convert<Function::kScale, FuncScale> {
    double onConvert(double value) override;
};
} // namespace

//===========================================================================
double FuncScale::onConvert(double value) {
    auto factor = m_args[0].number;
    return value * factor;
}


/****************************************************************************
*
*   FuncTimeShift
*
***/

namespace {
class FuncTimeShift : public FuncImpl<Function::kTimeShift, FuncTimeShift> {
    bool onFuncBind() override;
    void onFuncAdjustRange(
        TimePoint * first,
        TimePoint * last,
        Duration * pretime,
        unsigned * presamples
    ) override;
    Apply onFuncApply(ResultInfo & info) override;

    Duration m_shift{};
};
} // namespace

//===========================================================================
bool FuncTimeShift::onFuncBind() {
    auto tmp = string(m_args[0].string.get());
    if (tmp[0] != '+' && tmp[0] != '-')
        tmp = "-" + tmp;
    if (!parse(&m_shift, tmp.c_str()))
        return false;
    return true;
}

//===========================================================================
void FuncTimeShift::onFuncAdjustRange(
    TimePoint * first,
    TimePoint * last,
    Duration * pretime,
    unsigned * presamples
) {
    *first += m_shift;
    *last += m_shift;
}

//===========================================================================
FuncNode::Apply FuncTimeShift::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(type(), info.name);
    info.samples = SampleList::dup(*info.samples);
    auto & first = info.samples->first;
    first -= m_shift;
    first -= first.time_since_epoch() % info.samples->interval;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncHighestCurrent
*
***/

namespace {
class FuncHighestCurrent
    : public FuncImpl<Function::kHighestCurrent, FuncHighestCurrent>
{
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

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

namespace {
class FuncHighestMax : public FuncImpl<Function::kHighestMax, FuncHighestMax> {
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

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
*   Aggregate - combined samples for a single time interval
*
***/

namespace {
template<Function::Type FT, typename T>
class Aggregate : public FuncImpl<FT, T> {
    FuncNode::Apply onResultTask(ResultInfo & info) override;

    virtual void onAggregate(double & agg, double newVal) = 0;
protected:
    shared_ptr<SampleList> m_samples;
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
FuncNode::Apply Aggregate<FT, T>::onResultTask(ResultInfo & info) {
    if (info.samples) {
        if (!m_samples) {
            m_samples = SampleList::dup(*info.samples);
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
                auto tmp = SampleList::alloc(
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
                auto ival = info.samples->samples[ipos];
                onAggregate(sval, ival);
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
        out.name = addFuncName(this->type(), info.target);
        out.samples = move(m_samples);
        out.more = false;
        this->forwardResult(out);
    }
    return FuncNode::Apply::kSkip;
}

//===========================================================================
// sum
//===========================================================================
namespace {
class FuncSum : public Aggregate<Function::kSum, FuncSum> {
    void onAggregate(double & agg, double newVal) override;
};
} // namespace

//===========================================================================
void FuncSum::onAggregate(double & agg, double newVal) {
    if (isnan(agg)) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg += newVal;
    }
}

//===========================================================================
// maxSeries
//===========================================================================
namespace {
class FuncMaxSeries : public Aggregate<Function::kMaxSeries, FuncMaxSeries> {
    void onAggregate(double & agg, double newVal) override;
};
} // namespace

//===========================================================================
void FuncMaxSeries::onAggregate(double & agg, double newVal) {
    if (isnan(agg) || newVal > agg)
        agg = newVal;
}

//===========================================================================
// minSeries
//===========================================================================
namespace {
class FuncMinSeries : public Aggregate<Function::kMinSeries, FuncMinSeries> {
    void onAggregate(double & agg, double newVal) override;
};
} // namespace

//===========================================================================
void FuncMinSeries::onAggregate(double & agg, double newVal) {
    if (isnan(agg) || newVal < agg)
        agg = newVal;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
// This function exists to trigger the static initializers in this file that
// then register the function node factories.
void Eval::initializeFuncs()
{}
