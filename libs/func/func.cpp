// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// func.cpp - tismet func
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

template<typename T>
class FuncFactory : public IFuncFactory {
public:
    using IFuncFactory::IFuncFactory;
    unique_ptr<IFuncInstance> onFactoryCreate() override;

    FuncFactory<T> & arg(
        string_view name,
        FuncArgInfo::Type type,
        bool require = false,
        bool multiple = false
    ) override;
    FuncFactory<T> & alias(string_view name);
};

template<typename T>
class IFuncBase : public IFuncInstance {
public:
    using Factory = FuncFactory<T>;

public:
    Function::Type type() const override;

    IFuncInstance * onFuncBind(std::vector<FuncArg> && args) override;
    void onFuncAdjustRange(
        TimePoint * first,
        TimePoint * last,
        Duration * pretime,
        unsigned * presamples
    ) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override = 0;

protected:
    Duration m_pretime{};
    unsigned m_presamples{0};

private:
    friend Factory;
    Duration m_oldPretime{};
    Function::Type m_type{};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static List<IFuncFactory> s_factories;


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
    auto fname = string_view(toString(ftype, "UNKNOWN"));
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
*   IFuncFactory
*
***/

//===========================================================================
IFuncFactory::IFuncFactory(string_view name, string_view group)
    : m_group{group}
{
    s_factories.link(this);
    m_names.push_back(string{name});
}

//===========================================================================
IFuncFactory::IFuncFactory(const IFuncFactory & from)
    : m_type(from.m_type)
    , m_names(from.m_names)
    , m_group(from.m_group)
    , m_args(from.m_args)
{
    s_factories.link(this);
}

//===========================================================================
IFuncFactory::IFuncFactory(IFuncFactory && from)
    : m_type(from.m_type)
    , m_names(move(from.m_names))
    , m_group(move(from.m_group))
    , m_args(move(from.m_args))
{
    s_factories.link(this);
}

//===========================================================================
IFuncFactory & IFuncFactory::arg(
    string_view name,
    FuncArgInfo::Type type,
    bool require,
    bool multiple
) {
    m_args.push_back(FuncArgInfo{string(name), type, require, multiple});
    return *this;
}


/****************************************************************************
*
*   FuncFactory
*
***/

//===========================================================================
template<typename T>
FuncFactory<T> & FuncFactory<T>::arg(
    string_view name,
    FuncArgInfo::Type type,
    bool require,
    bool multiple
) {
    IFuncFactory::arg(name, type, require, multiple);
    return *this;
}

//===========================================================================
template<typename T>
FuncFactory<T> & FuncFactory<T>::alias(string_view name) {
    m_names.push_back(string(name));
    return *this;
}

//===========================================================================
template<typename T>
unique_ptr<IFuncInstance> FuncFactory<T>::onFactoryCreate() {
    auto ptr = make_unique<T>();
    ptr->m_type = m_type;
    return move(ptr);
}


/****************************************************************************
*
*   IFuncBase
*
***/

//===========================================================================
template<typename T>
Function::Type IFuncBase<T>::type() const {
    return m_type;
}

//===========================================================================
template<typename T>
IFuncInstance * IFuncBase<T>::onFuncBind(vector<FuncArg> && args) {
    return this;
}

//===========================================================================
template<typename T>
void IFuncBase<T>::onFuncAdjustRange(
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
*   PassthruBase
*
***/

namespace {
class PassthruBase : public IFuncBase<PassthruBase> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
};
} // namespace

//===========================================================================
bool PassthruBase::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    notify->onFuncOutput(info);
    return true;
}

static auto s_aliasSub = PassthruBase::Factory("aliasSub", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("search", FuncArgInfo::kString, true)
    .arg("replace", FuncArgInfo::kString, true);
static auto s_color = PassthruBase::Factory("color", "Graph")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("color", FuncArgInfo::kString, true);
static auto s_legendValue = PassthruBase::Factory("legendValue", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("valuesTypes", FuncArgInfo::kString, false, true);
static auto s_lineWidth = PassthruBase::Factory("lineWidth", "Graph")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("width", FuncArgInfo::kNum, true);


/****************************************************************************
*
*   FuncAlias
*
***/

namespace {
class FuncAlias : public IFuncBase<FuncAlias> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    shared_ptr<char[]> m_name;
};
} // namespace
static auto s_alias = FuncAlias::Factory("alias", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("name", FuncArgInfo::kString, true);

//===========================================================================
IFuncInstance * FuncAlias::onFuncBind(vector<FuncArg> && args) {
    m_name = args[0].string;
    return this;
}

//===========================================================================
bool FuncAlias::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    info.name = m_name;
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   Rebinding function implementations
*
***/

namespace {
class FuncAggregate : public IFuncBase<FuncAggregate> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
};
} // namespace
static auto s_aggregate = FuncAggregate::Factory("aggregate", "Combine")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("aggFunc", FuncArgInfo::kAggFunc, true);

//===========================================================================
IFuncInstance * FuncAggregate::onFuncBind(vector<FuncArg> && args) {
    auto aggtype = fromString(args[0].string.get(), Aggregate::kAverage);
    auto fname = string(toString(aggtype, "")) + "Series";
    args.erase(args.begin());
    auto type = fromString(fname, Function::kSumSeries);
    auto func = funcCreate(type);
    auto bound = func->onFuncBind(move(args));
    if (bound == func.get())
        func.release();
    return bound;
}

//===========================================================================
bool FuncAggregate::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    assert(!"onFuncApply not implemented by aggregate base function");
    return false;
}


/****************************************************************************
*
*   FuncConsolidateBy
*
***/

namespace {
class FuncConsolidateBy : public IFuncBase<FuncConsolidateBy> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    Aggregate::Type m_method;
};
} // namespace
static auto s_consolidateBy =
    FuncConsolidateBy::Factory("consolidateBy", "Special")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("method", FuncArgInfo::kAggFunc, true);

//===========================================================================
IFuncInstance * FuncConsolidateBy::onFuncBind(vector<FuncArg> && args) {
    m_method = fromString(args[0].string.get(), Aggregate::Type{});
    return this;
}

//===========================================================================
bool FuncConsolidateBy::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    info.method = m_method;
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   IFilterBase - exclude sample lists from results
*
***/

namespace {
template<typename T>
class IFilterBase : public IFuncBase<T> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual bool onFilter(const ResultInfo & info) = 0;
};
} // namespace

//===========================================================================
template<typename T>
bool IFilterBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (!info.samples || onFilter(info))
        notify->onFuncOutput(info);
    return true;
}

//===========================================================================
// maximumAbove
//===========================================================================
namespace {
class FuncMaximumAbove : public IFilterBase<FuncMaximumAbove> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFilter(const ResultInfo & info) override;
    double m_limit{};
};
} // namespace
static auto s_maximumAbove =
    FuncMaximumAbove::Factory("maximumAbove", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncMaximumAbove::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return this;
}

//===========================================================================
bool FuncMaximumAbove::onFilter(const ResultInfo & info) {
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    return find_if(ptr, eptr, [&](auto val){ return val > m_limit; }) != eptr;
}


/****************************************************************************
*
*   ITransformBase - make changes within a single SampleList
*
***/

namespace {
template<typename T>
class ITransformBase : public IFuncBase<T> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) = 0;
};
} // namespace

//===========================================================================
template<typename T>
bool ITransformBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(this->type(), info.name);
        auto out = SampleList::alloc(*info.samples);
        auto optr = out->samples;
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        onTransform(optr, ptr, eptr, out->interval);
        info.samples = out;
    }
    notify->onFuncOutput(info);
    return true;
}

//===========================================================================
// derivative
//===========================================================================
namespace {
class FuncDerivative : public ITransformBase<FuncDerivative> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) override;
};
} // namespace
static auto s_derivative = FuncDerivative::Factory("derivative", "Transform")
    .arg("query", FuncArgInfo::kQuery, true);

//===========================================================================
IFuncInstance * FuncDerivative::onFuncBind(vector<FuncArg> && args) {
    m_presamples = 1;
    return this;
}

//===========================================================================
void FuncDerivative::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr,
    Duration interval
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
class FuncKeepLastValue : public ITransformBase<FuncKeepLastValue> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) override;

    double m_limit;
};
} // namespace
static auto s_keepLastValue =
    FuncKeepLastValue::Factory("keepLastValue", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("limit", FuncArgInfo::kNum);

//===========================================================================
IFuncInstance * FuncKeepLastValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args.empty() ? 0 : (int) args[0].number;
    m_presamples = 1;
    return this;
}

//===========================================================================
void FuncKeepLastValue::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr,
    Duration interval
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
class FuncMovingAverage : public ITransformBase<FuncMovingAverage> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) override;

    unsigned m_count{};
};
} // namespace
static auto s_movingAverage =
    FuncMovingAverage::Factory("movingAverage", "Calculate")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("windowSize", FuncArgInfo::kNumOrString, true)
    .arg("xFilesFactor", FuncArgInfo::kNum, false);

//===========================================================================
IFuncInstance * FuncMovingAverage::onFuncBind(vector<FuncArg> && args) {
    if (auto arg0 = args[0].string.get()) {
        if (parse(&m_pretime, arg0))
            return this;
        m_presamples = strToUint(arg0);
    } else {
        m_presamples = (unsigned) args[0].number;
    }
    if (m_presamples)
        m_presamples -= 1;
    return this;
}

//===========================================================================
void FuncMovingAverage::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr,
    Duration interval
) {
    unsigned count;
    if (m_pretime.count()) {
        auto pretime = m_pretime - m_pretime % interval;
        count = unsigned(pretime / interval) + 1;
    } else {
        count = m_presamples + 1;
    }

    auto pre = ptr;
    assert(pre + count <= eptr);
    double sum = 0;
    unsigned nans = 0;
    for (unsigned i = 1; i <= count; ++i, ++ptr) {
        if (isnan(*ptr)) {
            if (++nans == i) {
                *optr++ = NAN;
                continue;
            }
        } else {
            sum += *ptr;
        }
        *optr++ = sum / count;
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
        *optr++ = (nans == count) ? NAN : sum / count;
    }
}

//===========================================================================
// nonNegativeDerivative
//===========================================================================
namespace {
class FuncNonNegativeDerivative
    : public ITransformBase<FuncNonNegativeDerivative>
{
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) override;

    double m_limit;
};
} // namespace
static auto s_nonNegativeDerivative =
    FuncNonNegativeDerivative::Factory("nonNegativeDerivative", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("maxValue", FuncArgInfo::kNum);

//===========================================================================
IFuncInstance * FuncNonNegativeDerivative::onFuncBind(vector<FuncArg> && args) {
    m_limit = args.empty() ? HUGE_VAL : args[0].number;
    m_presamples = 1;
    return this;
}

//===========================================================================
void FuncNonNegativeDerivative::onTransform(
    double * optr,
    const double * ptr,
    const double * eptr,
    Duration interval
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
*   IConvertBase - changes to individual samples
*
***/

namespace {
template<typename T>
class IConvertBase : public IFuncBase<T> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual double onConvert(double value) = 0;
    virtual void onConvertStart(Duration interval) {}
};
} // namespace

//===========================================================================
template<typename T>
bool IConvertBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
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
    notify->onFuncOutput(info);
    return true;
}

//===========================================================================
// absolute
//===========================================================================
namespace {
class FuncAbsolute : public IConvertBase<FuncAbsolute> {
    double onConvert(double value) override;
};
} // namespace
static auto s_absolute = FuncAbsolute::Factory("absolute", "Transform")
    .arg("query", FuncArgInfo::kQuery, true);

//===========================================================================
double FuncAbsolute::onConvert(double value) {
    return abs(value);
}

//===========================================================================
// drawAsInfinite
//===========================================================================
namespace {
class FuncDrawAsInfinite : public IConvertBase<FuncDrawAsInfinite> {
    double onConvert(double value) override;
};
} // namespace
static auto s_drawAsInfinite =
    FuncDrawAsInfinite::Factory("drawAsInfinite", "Transform")
    .arg("query", FuncArgInfo::kQuery, true);

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
class FuncRemoveAboveValue : public IConvertBase<FuncRemoveAboveValue> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeAboveValue =
    FuncRemoveAboveValue::Factory("removeAboveValue", "Filter Data")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveAboveValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return this;
}

//===========================================================================
double FuncRemoveAboveValue::onConvert(double value) {
    return value > m_limit ? NAN : value;
}

//===========================================================================
// removeBelowValue
//===========================================================================
namespace {
class FuncRemoveBelowValue : public IConvertBase<FuncRemoveBelowValue> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeBelowValue =
    FuncRemoveBelowValue::Factory("removeBelowValue", "Filter Data")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveBelowValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return this;
}

//===========================================================================
double FuncRemoveBelowValue::onConvert(double value) {
    return value < m_limit ? NAN : value;
}

//===========================================================================
// scale
//===========================================================================
namespace {
class FuncScale : public IConvertBase<FuncScale> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_factor;
};
} // namespace
static auto s_scale = FuncScale::Factory("scale", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("factor", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncScale::onFuncBind(vector<FuncArg> && args) {
    m_factor = args[0].number;
    return this;
}

//===========================================================================
double FuncScale::onConvert(double value) {
    return value * m_factor;
}

//===========================================================================
// scaleToSeconds
//===========================================================================
namespace {
class FuncScaleToSeconds : public IConvertBase<FuncScaleToSeconds> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    void onConvertStart(Duration interval) override;

    double m_seconds;
    double m_factor;
};
} // namespace
static auto s_scaleToSeconds =
    FuncScaleToSeconds::Factory("scaleToSeconds", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("seconds", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncScaleToSeconds::onFuncBind(vector<FuncArg> && args) {
    m_seconds = args[0].number;
    return this;
}

//===========================================================================
void FuncScaleToSeconds::onConvertStart(Duration interval) {
    m_factor = m_seconds / duration_cast<seconds>(interval).count();
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
class FuncTimeShift : public IFuncBase<FuncTimeShift> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onFuncAdjustRange(
        TimePoint * first,
        TimePoint * last,
        Duration * pretime,
        unsigned * presamples
    ) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    Duration m_shift{};
};
} // namespace
static auto s_timeShift = FuncTimeShift::Factory("timeShift", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("timeShift", FuncArgInfo::kString, true);

//===========================================================================
IFuncInstance * FuncTimeShift::onFuncBind(vector<FuncArg> && args) {
    auto tmp = string(args[0].string.get());
    if (tmp[0] != '+' && tmp[0] != '-')
        tmp = "-" + tmp;
    if (!parse(&m_shift, tmp.c_str()))
        return nullptr;
    return this;
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
bool FuncTimeShift::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(type(), info.name);
        info.samples = SampleList::dup(*info.samples);
        auto & first = info.samples->first;
        first -= m_shift;
        first -= first.time_since_epoch() % info.samples->interval;
    }
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   FuncHighestCurrent
*
***/

namespace {
class FuncHighestCurrent : public IFuncBase<FuncHighestCurrent> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
    unsigned m_allowed;
};
} // namespace
static auto s_highestCurrent =
    FuncHighestCurrent::Factory("highestCurrent", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncHighestCurrent::onFuncBind(vector<FuncArg> && args) {
    m_allowed = (unsigned) args[0].number;
    return this;
}

//===========================================================================
bool FuncHighestCurrent::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    // last non-NAN sample in list
    auto best = (double) NAN;
    if (info.samples) {
        for (int i = info.samples->count; i-- > 0;) {
            best = info.samples->samples[i];
            if (!isnan(best))
                break;
        }
        if (!isnan(best)) {
            if (m_best.size() < m_allowed) {
                m_best.emplace(best, info);
            } else if (auto i = m_best.begin();  i->first < best) {
                m_best.erase(i);
                m_best.emplace(best, info);
            }
        }
    } else {
        for (auto && out : m_best)
            notify->onFuncOutput(out.second);
        info.name = {};
        info.samples = {};
        notify->onFuncOutput(info);
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
class FuncHighestMax : public IFuncBase<FuncHighestMax> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
    unsigned m_allowed;
};
} // namespace
static auto s_highestMax =
    FuncHighestMax::Factory("highestMax", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
IFuncInstance * FuncHighestMax::onFuncBind(vector<FuncArg> && args) {
    m_allowed = (unsigned) args[0].number;
    return this;
}

//===========================================================================
bool FuncHighestMax::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    // largest non-NAN sample in list
    auto best = -numeric_limits<double>::infinity();
    bool found = false;
    if (info.samples) {
        for (auto && ref : *info.samples) {
            if (ref > best) {
                best = ref;
                found = true;
            }
        }
        if (found) {
            if (m_best.size() < m_allowed) {
                m_best.emplace(best, info);
            } else if (auto i = m_best.begin();  best > i->first) {
                m_best.erase(i);
                m_best.emplace(best, info);
            }
        }
    } else {
        for (auto i = m_best.rbegin(), ei = m_best.rend(); i != ei; ++i)
            notify->onFuncOutput(i->second);
        info.name = {};
        info.samples = {};
        notify->onFuncOutput(info);
        m_best.clear();
    }
    return true;
}


/****************************************************************************
*
*   ICombineBase - combine samples for a single time interval
*
***/

namespace {
template<typename T>
class ICombineBase : public IFuncBase<T> {
protected:
    using impl_type = ICombineBase;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual void onCombineApply(
        ResultInfo & info,
        TimePoint last,
        TimePoint sfirst
    );
    virtual void onCombineResize(int prefix, int suffix) {}
    virtual void onCombineValue(double & agg, int pos, double newVal) {};
    virtual void onCombineFinalize() {}
    virtual void onCombineClear() {}
protected:
    shared_ptr<SampleList> m_samples;
};
} // namespace

//===========================================================================
template<typename T>
bool ICombineBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples) {
        if (!m_samples) {
            m_samples = SampleList::dup(*info.samples);
            onCombineResize(0, m_samples->count);
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
                auto prefix = pos;
                auto spos = 0;
                for (; first < slast; first += interval, ++pos, ++spos)
                    tmp->samples[pos] = m_samples->samples[spos];
                auto postfix = pos;
                for (; first < last; first += interval, ++pos)
                    tmp->samples[pos] = NAN;
                assert(pos == (int) tmp->count);
                m_samples = tmp;
                onCombineResize(prefix, m_samples->count - postfix);
                slast = first;
                first = m_samples->first;
            }
            onCombineApply(info, ilast, first);
        } else {
            // TODO: normalize and consolidate incompatible lists
            logMsgError() << "Aggregating incompatible series, "
                << info.name.get();
        }
        return true;
    }

    onCombineFinalize();

    // Output aggregated result and end mark
    info.name = addFuncName(this->type(), info.target);
    info.samples = move(m_samples);
    notify->onFuncOutput(info);
    info.name = {};
    info.samples = {};
    notify->onFuncOutput(info);

    onCombineClear();
    m_samples.reset();
    return true;
}

//===========================================================================
template<typename T>
void ICombineBase<T>::onCombineApply(
    ResultInfo & info,
    TimePoint last,
    TimePoint sfirst
) {
    auto interval = info.samples->interval;
    auto pos = 0;
    auto first = info.samples->first;
    auto spos = (first - sfirst) / interval;
    for (; first < last; first += interval, ++pos, ++spos) {
        auto & sval = m_samples->samples[spos];
        auto val = info.samples->samples[pos];
        onCombineValue(sval, (int) spos, val);
    }
    assert(spos <= m_samples->count);
}

//===========================================================================
// averageSeries
//===========================================================================
namespace {
class FuncAverageSeries : public ICombineBase<FuncAverageSeries> {
    void onCombineResize(int prefix, int postfix) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    void onCombineClear() override;
    vector<unsigned> m_counts;
};
} // namespace
static auto s_averageSeries =
    FuncAverageSeries::Factory("averageSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true)
    .alias("avg");

//===========================================================================
void FuncAverageSeries::onCombineResize(int prefix, int postfix) {
    m_counts.insert(m_counts.begin(), prefix, 1);
    m_counts.resize(m_counts.size() + postfix, 1);
}

//===========================================================================
void FuncAverageSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (!isnan(newVal)) {
        auto cnt = m_counts[pos]++;
        agg = (agg * (cnt - 1) + newVal) / cnt;
    }
}

//===========================================================================
void FuncAverageSeries::onCombineClear() {
    m_counts.clear();
}

//===========================================================================
// countSeries
//===========================================================================
namespace {
class FuncCountSeries : public ICombineBase<FuncCountSeries> {
    void onCombineApply(
        ResultInfo & info,
        TimePoint last,
        TimePoint sfirst
    ) override;
    void onCombineFinalize() override;
    void onCombineClear() override;
    unsigned m_count{1};
};
} // namespace
static auto s_countSeries =
    FuncCountSeries::Factory("countSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncCountSeries::onCombineApply(
    ResultInfo & info,
    TimePoint last,
    TimePoint sfirst
) {
    m_count += 1;
}

//===========================================================================
void FuncCountSeries::onCombineFinalize() {
    for (auto && ref : *m_samples)
        ref = m_count;
}

//===========================================================================
void FuncCountSeries::onCombineClear() {
    m_count = 1;
}

//===========================================================================
// diffSeries
//===========================================================================
namespace {
class FuncDiffSeries : public ICombineBase<FuncDiffSeries> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    bool m_firstArg{false};
};
} // namespace
static auto s_diffSeries = FuncDiffSeries::Factory("diffSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
bool FuncDiffSeries::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    m_firstArg = (info.argPos == 0);
    if (!info.samples || m_samples)
        return impl_type::onFuncApply(notify, info);

    m_samples = SampleList::dup(*info.samples);
    if (!m_firstArg) {
        for (auto && ref : *m_samples) {
            if (isnan(ref)) {
                ref = 0;
            } else {
                ref = -ref;
            }
        }
    }
    onCombineResize(0, m_samples->count);
    return true;
}

//===========================================================================
void FuncDiffSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (m_firstArg) {
        if (isnan(newVal)) {
            agg = newVal;
        } else {
            agg += newVal;
        }
    } else if (!isnan(newVal)) {
        agg -= newVal;
    }
}

//===========================================================================
// lastSeries
//===========================================================================
namespace {
class FuncLastSeries : public ICombineBase<FuncLastSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_lastSeries =
    FuncLastSeries::Factory("lastSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncLastSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (!isnan(newVal))
        agg = newVal;
}

//===========================================================================
// maxSeries
//===========================================================================
namespace {
class FuncMaxSeries : public ICombineBase<FuncMaxSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_maxSeries = FuncMaxSeries::Factory("maxSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMaxSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal > agg)
        agg = newVal;
}

//===========================================================================
// medianSeries
//===========================================================================
namespace {
class FuncMedianSeries : public ICombineBase<FuncMedianSeries> {
    void onCombineResize(int prefix, int postfix) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    void onCombineFinalize() override;
    void onCombineClear() override;
    vector<vector<double>> m_samplesByPos;
};
} // namespace
static auto s_medianSeries = FuncMedianSeries::Factory("medianSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMedianSeries::onCombineResize(int prefix, int postfix) {
    if (m_samplesByPos.empty()) {
        assert(!prefix);
        m_samplesByPos.resize(postfix);
        for (auto i = 0; i < postfix; ++i)
            m_samplesByPos[i].push_back(m_samples->samples[i]);
        return;
    }
    vector<double> val(m_samplesByPos[0].size(), NAN);
    m_samplesByPos.insert(m_samplesByPos.begin(), prefix, val);
    m_samplesByPos.resize(m_samplesByPos.size() + postfix, val);
}

//===========================================================================
void FuncMedianSeries::onCombineValue(double & agg, int pos, double newVal) {
    m_samplesByPos[pos].push_back(newVal);
}

//===========================================================================
void FuncMedianSeries::onCombineFinalize() {
    for (auto i = 0; i < m_samplesByPos.size(); ++i) {
        auto & agg = m_samples->samples[i];
        agg = aggMedian(m_samplesByPos[i].data(), m_samplesByPos[i].size());
    }
}

//===========================================================================
void FuncMedianSeries::onCombineClear() {
    m_samplesByPos.clear();
}

//===========================================================================
// minSeries
//===========================================================================
namespace {
class FuncMinSeries : public ICombineBase<FuncMinSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_minSeries = FuncMinSeries::Factory("minSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMinSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal < agg)
        agg = newVal;
}

//===========================================================================
// multiplySeries
//===========================================================================
namespace {
class FuncMultiplySeries : public ICombineBase<FuncMultiplySeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_multiplySeries =
    FuncMultiplySeries::Factory("multiplySeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMultiplySeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg)) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg *= newVal;
    }
}

//===========================================================================
// rangeSeries
//===========================================================================
namespace {
class FuncRangeSeries : public ICombineBase<FuncRangeSeries> {
    void onCombineResize(int prefix, int postfix) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    void onCombineFinalize() override;
    void onCombineClear() override;
    vector<double> m_minSamples;
};
} // namespace
static auto s_rangeSeries = FuncRangeSeries::Factory("rangeSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncRangeSeries::onCombineResize(int prefix, int postfix) {
    m_minSamples.insert(m_minSamples.begin(), prefix, NAN);
    m_minSamples.resize(m_minSamples.size() + postfix, NAN);
}

//===========================================================================
void FuncRangeSeries::onCombineValue(double & agg, int pos, double newVal) {
    auto & aggMin = m_minSamples[pos];
    if (isnan(agg)) {
        agg = aggMin = newVal;
    } else {
        if (newVal > agg)
            agg = newVal;
        if (newVal < aggMin)
            aggMin = newVal;
    }
}

//===========================================================================
void FuncRangeSeries::onCombineFinalize() {
    for (auto i = 0; i < m_minSamples.size(); ++i) {
        auto & agg = m_samples->samples[i];
        agg -= m_minSamples[i];
    }
}

//===========================================================================
void FuncRangeSeries::onCombineClear() {
    m_minSamples.clear();
}

//===========================================================================
// stddevSeries
//===========================================================================
namespace {
class FuncStddevSeries : public ICombineBase<FuncStddevSeries> {
    void onCombineResize(int prefix, int postfix) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    void onCombineFinalize() override;
    void onCombineClear() override;
    struct Info {
        double mean;
        unsigned count;
    };
    vector<Info> m_infos;
};
} // namespace
static auto s_stddevSeries =
    FuncStddevSeries::Factory("stddevSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncStddevSeries::onCombineResize(int prefix, int postfix) {
    m_infos.insert(m_infos.begin(), prefix, {});
    m_infos.resize(m_infos.size() + postfix, {});
    auto fn = [&](auto pos) {
        auto & agg = m_samples->samples[pos];
        auto & info = m_infos[pos];
        if (!isnan(agg)) {
            info.count = 1;
            info.mean = agg;
            agg = 0;
        }
    };
    for (auto pos = 0; pos < prefix; ++pos)
        fn(pos);
    for (auto pos = m_infos.size() - postfix; pos < m_infos.size(); ++pos)
        fn(pos);
}

//===========================================================================
void FuncStddevSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (!isnan(newVal)) {
        auto & info = m_infos[pos];
        if (++info.count == 1) {
            info.mean = newVal;
            agg = 0;
        } else {
            auto mean = info.mean + (newVal - info.mean) / info.count;
            agg = agg + (newVal - info.mean) * (newVal - mean);
            info.mean = mean;
        }
    }
}

//===========================================================================
void FuncStddevSeries::onCombineFinalize() {
    int pos = 0;
    for (auto && info : m_infos) {
        auto & agg = m_samples->samples[pos++];
        agg = sqrt(agg / info.count);
    }
}

//===========================================================================
void FuncStddevSeries::onCombineClear() {
    m_infos.clear();
}

//===========================================================================
// sumSeries
//===========================================================================
namespace {
class FuncSumSeries : public ICombineBase<FuncSumSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_sumSeries = FuncSumSeries::Factory("sumSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true)
    .alias("sum");

//===========================================================================
void FuncSumSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg)) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg += newVal;
    }
}


/****************************************************************************
*
*   Private
*
***/

static vector<IFuncFactory *> s_funcVec;
static vector<TokenTable::Token> s_funcTokens;
static TokenTable s_funcTbl = [](){
    for (auto && f : s_factories)
        s_funcVec.push_back(&f);
    sort(s_funcVec.begin(), s_funcVec.end(), [](auto & a, auto & b) {
        return a->m_names.front() < b->m_names.front();
    });
    for (unsigned i = 0; i < s_funcVec.size(); ++i) {
        auto & f = *s_funcVec[i];
        f.m_type = (Function::Type) i;
        for (auto && n : f.m_names) {
            auto & token = s_funcTokens.emplace_back();
            token.id = i;
            token.name = n.c_str();
        }
    }
    return TokenTable{s_funcTokens.data(), s_funcTokens.size()};
}();


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
const TokenTable & funcEnums() {
    return s_funcTbl;
}

//===========================================================================
const List<IFuncFactory> & funcFactories() {
    return s_factories;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
unique_ptr<IFuncInstance> funcCreate(Function::Type type) {
    assert(type < s_funcVec.size());
    return s_funcVec[type]->onFactoryCreate();
}

//===========================================================================
const char * toString(Eval::Function::Type ftype, const char def[]) {
    return tokenTableGetName(s_funcTbl, ftype, def);
}

//===========================================================================
Function::Type fromString(string_view src, Function::Type def) {
    return tokenTableGetEnum(s_funcTbl, src, def);
}

