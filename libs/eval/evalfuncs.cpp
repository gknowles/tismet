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
    bool onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
bool FuncAlias::onFuncApply(ResultInfo & info) {
    info.name = m_args[0].string;
    outputResult(info);
    return true;
}


/****************************************************************************
*
*   Filter - exclude sample lists from results
*
***/

namespace {
template<Function::Type FT, typename T>
class Filter : public FuncImpl<FT, T> {
    bool onFuncApply(ResultInfo & info) override;

    virtual bool onFilter(const ResultInfo & info) = 0;
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
bool Filter<FT, T>::onFuncApply(ResultInfo & info) {
    if (!info.samples || onFilter(info))
        this->outputResult(info);
    return true;
}

//===========================================================================
// maximumAbove
//===========================================================================
namespace {
class FuncMaximumAbove
    : public Filter<Function::kMaximumAbove, FuncMaximumAbove>
{
    bool onFilter(const ResultInfo & info) override;
};
} // namespace

//===========================================================================
bool FuncMaximumAbove::onFilter(const ResultInfo & info) {
    auto limit = m_args[0].number;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    return find_if(ptr, eptr, [&](auto val){ return val > limit; }) != eptr;
}


/****************************************************************************
*
*   Transform - make changes within a single SampleList
*
***/

namespace {
template<Function::Type FT, typename T>
class Transform : public FuncImpl<FT, T> {
    bool onFuncApply(ResultInfo & info) override;

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
bool Transform<FT, T>::onFuncApply(ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(this->type(), info.name);
        auto out = SampleList::alloc(*info.samples);
        onTransformStart(out->interval);
        auto optr = out->samples;
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        onTransform(optr, ptr, eptr);
        info.samples = out;
    }
    this->outputResult(info);
    return true;
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

    unsigned m_count{};
};
} // namespace

//===========================================================================
bool FuncMovingAverage::onFuncBind() {
    if (auto arg0 = m_args[0].string.get()) {
        if (parse(&m_pretime, arg0))
            return true;
        m_presamples = strToUint(arg0);
    } else {
        m_presamples = (unsigned) m_args[0].number;
    }
    if (m_presamples)
        m_presamples -= 1;
    return true;
}

//===========================================================================
void FuncMovingAverage::onTransformStart(Duration interval) {
    if (m_pretime.count()) {
        auto pretime = m_pretime - m_pretime % interval;
        m_count = unsigned(pretime / interval) + 1;
        return;
    }

    m_count = m_presamples + 1;
}

//===========================================================================
void FuncMovingAverage::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr
) {
    auto pre = ptr;
    auto epre = pre + m_count;
    assert(epre <= eptr);
    double sum = 0;
    unsigned nans = 0;
    for (unsigned i = 1; i <= m_count; ++i, ++ptr) {
        if (isnan(*ptr)) {
            if (++nans == i) {
                *optr++ = NAN;
                continue;
            }
        } else {
            sum += *ptr;
        }
        *optr++ = sum / m_count;
    }
    for (; ptr != eptr; ++ptr, ++pre) {
        if (isnan(*ptr)) {
            nans += 1;
        } else {
            sum += *ptr;
        }
        if (isnan(*pre)) {
            nans -= 1;
        } else {
            sum -= *pre;
        }
        *optr++ = (nans == m_count) ? NAN : sum / m_count;
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
    bool onFuncApply(ResultInfo & info) override;

    virtual double onConvert(double value) = 0;
    virtual void onConvertStart(Duration interval) {}
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
bool Convert<FT, T>::onFuncApply(ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(this->type(), info.name);
        auto out = SampleList::alloc(*info.samples);
        onConvertStart(out->interval);
        auto optr = out->samples;
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        for (; ptr != eptr; ++ptr)
            *optr++ = onConvert(*ptr);
        assert(optr == out->samples + out->count);
        info.samples = out;
    }
    this->outputResult(info);
    return true;
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

//===========================================================================
// scaleToSeconds
//===========================================================================
namespace {
class FuncScaleToSeconds
    : public Convert<Function::kScaleToSeconds, FuncScaleToSeconds>
{
    double onConvert(double value) override;
    void onConvertStart(Duration interval) override;

    double m_factor;
};
} // namespace

//===========================================================================
void FuncScaleToSeconds::onConvertStart(Duration interval) {
    m_factor = m_args[0].number / duration_cast<seconds>(interval).count();
}

//===========================================================================
double FuncScaleToSeconds::onConvert(double value) {
    return value * m_factor;
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
    bool onFuncApply(ResultInfo & info) override;

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
bool FuncTimeShift::onFuncApply(ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(type(), info.name);
        info.samples = SampleList::dup(*info.samples);
        auto & first = info.samples->first;
        first -= m_shift;
        first -= first.time_since_epoch() % info.samples->interval;
    }
    outputResult(info);
    return true;
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
    bool onFuncApply(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

//===========================================================================
bool FuncHighestCurrent::onFuncApply(ResultInfo & info) {
    auto allowed = m_args[0].number;

    // last non-NAN sample in list
    auto best = (double) NAN;
    if (info.samples) {
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
    } else {
        for (auto && out : m_best)
            outputResult(out.second);
        info.name = {};
        info.samples = {};
        outputResult(info);
        m_best.clear();
    }
    return true;
}


/****************************************************************************
*
*   FuncHighestMax
*
***/

namespace {
class FuncHighestMax : public FuncImpl<Function::kHighestMax, FuncHighestMax> {
    bool onFuncApply(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

//===========================================================================
bool FuncHighestMax::onFuncApply(ResultInfo & info) {
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
        if (found) {
            if (m_best.size() < allowed) {
                m_best.emplace(best, info);
            } else if (auto i = m_best.begin();  best > i->first) {
                m_best.erase(i);
                m_best.emplace(best, info);
            }
        }
    } else {
        for (auto i = m_best.rbegin(), ei = m_best.rend(); i != ei; ++i)
            outputResult(i->second);
        info.name = {};
        info.samples = {};
        outputResult(info);
        m_best.clear();
    }
    return true;
}


/****************************************************************************
*
*   Aggregate - combined samples for a single time interval
*
***/

namespace {
template<Function::Type FT, typename T>
class Aggregate : public FuncImpl<FT, T> {
    bool onFuncApply(ResultInfo & info) override;

    virtual void onAggregate(double & agg, double newVal) = 0;
protected:
    shared_ptr<SampleList> m_samples;
};
} // namespace

//===========================================================================
template<Function::Type FT, typename T>
bool Aggregate<FT, T>::onFuncApply(ResultInfo & info) {
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
        return true;
    }

    // Output aggregated result and end mark
    info.name = addFuncName(this->type(), info.target);
    info.samples = move(m_samples);
    this->outputResult(info);
    info.name = {};
    info.samples = {};
    this->outputResult(info);
    return true;
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
