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

    bool onFuncBind(std::vector<FuncArg> && args) override;
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
bool IFuncBase<T>::onFuncBind(vector<FuncArg> && args) {
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    shared_ptr<char[]> m_name;
};
} // namespace
static auto s_alias = FuncAlias::Factory("alias", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("name", FuncArgInfo::kString, true);

//===========================================================================
bool FuncAlias::onFuncBind(vector<FuncArg> && args) {
    m_name = args[0].string;
    return true;
}

//===========================================================================
bool FuncAlias::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    info.name = m_name;
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   FuncConsolidateBy
*
***/

namespace {
class FuncConsolidateBy : public IFuncBase<FuncConsolidateBy> {
    bool onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    Aggregate::Type m_method;
};
} // namespace
static auto s_consolidateBy =
    FuncConsolidateBy::Factory("consolidateBy", "Special")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("method", FuncArgInfo::kString, true);

//===========================================================================
bool FuncConsolidateBy::onFuncBind(vector<FuncArg> && args) {
    m_method = fromString(args[0].string.get(), Aggregate::Type{});
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
    bool onFilter(const ResultInfo & info) override;
    double m_limit{};
};
} // namespace
static auto s_maximumAbove =
    FuncMaximumAbove::Factory("maximumAbove", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
bool FuncMaximumAbove::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return true;
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

    virtual void onTransformStart(Duration interval) {}
    virtual void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) = 0;
};
} // namespace

//===========================================================================
template<typename T>
bool ITransformBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
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
    notify->onFuncOutput(info);
    return true;
}

//===========================================================================
// derivative
//===========================================================================
namespace {
class FuncDerivative : public ITransformBase<FuncDerivative> {
    bool onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;
};
} // namespace
static auto s_derivative = FuncDerivative::Factory("derivative", "Transform")
    .arg("query", FuncArgInfo::kQuery, true);

//===========================================================================
bool FuncDerivative::onFuncBind(vector<FuncArg> && args) {
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
class FuncKeepLastValue : public ITransformBase<FuncKeepLastValue> {
    bool onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;

    double m_limit;
};
} // namespace
static auto s_keepLastValue =
    FuncKeepLastValue::Factory("keepLastValue", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("limit", FuncArgInfo::kNum);

//===========================================================================
bool FuncKeepLastValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args.empty() ? 0 : (int) args[0].number;
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
class FuncMovingAverage : public ITransformBase<FuncMovingAverage> {
    bool onFuncBind(vector<FuncArg> && args) override;
    void onTransformStart(Duration interval) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
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
bool FuncMovingAverage::onFuncBind(vector<FuncArg> && args) {
    if (auto arg0 = args[0].string.get()) {
        if (parse(&m_pretime, arg0))
            return true;
        m_presamples = strToUint(arg0);
    } else {
        m_presamples = (unsigned) args[0].number;
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
    assert(pre + m_count <= eptr);
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
    : public ITransformBase<FuncNonNegativeDerivative>
{
    bool onFuncBind(vector<FuncArg> && args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr
    ) override;

    double m_limit;
};
} // namespace
static auto s_nonNegativeDerivative =
    FuncNonNegativeDerivative::Factory("nonNegativeDerivative", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("maxValue", FuncArgInfo::kNum);

//===========================================================================
bool FuncNonNegativeDerivative::onFuncBind(vector<FuncArg> && args) {
    m_limit = args.empty() ? HUGE_VAL : args[0].number;
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
    bool onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeAboveValue =
    FuncRemoveAboveValue::Factory("removeAboveValue", "Filter Data")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
bool FuncRemoveAboveValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeBelowValue =
    FuncRemoveBelowValue::Factory("removeBelowValue", "Filter Data")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
bool FuncRemoveBelowValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_factor;
};
} // namespace
static auto s_scale = FuncScale::Factory("scale", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("factor", FuncArgInfo::kNum, true);

//===========================================================================
bool FuncScale::onFuncBind(vector<FuncArg> && args) {
    m_factor = args[0].number;
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
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
bool FuncScaleToSeconds::onFuncBind(vector<FuncArg> && args) {
    m_seconds = args[0].number;
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
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
bool FuncTimeShift::onFuncBind(vector<FuncArg> && args) {
    auto tmp = string(args[0].string.get());
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
    bool onFuncBind(vector<FuncArg> && args) override;
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
bool FuncHighestCurrent::onFuncBind(vector<FuncArg> && args) {
    m_allowed = (unsigned) args[0].number;
    return true;
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
    bool onFuncBind(vector<FuncArg> && args) override;
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
bool FuncHighestMax::onFuncBind(vector<FuncArg> && args) {
    m_allowed = (unsigned) args[0].number;
    return true;
}

//===========================================================================
bool FuncHighestMax::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
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
*   IAggregateBase - combined samples for a single time interval
*
***/

namespace {
template<typename T>
class IAggregateBase : public IFuncBase<T> {
protected:
    using impl_type = IAggregateBase;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual void onResize(int count) {}
    virtual void onAggregate(double & agg, int pos, double newVal) = 0;
    virtual void onFinalize() {}
protected:
    shared_ptr<SampleList> m_samples;
};
} // namespace

//===========================================================================
template<typename T>
bool IAggregateBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples) {
        if (!m_samples) {
            m_samples = SampleList::dup(*info.samples);
            onResize(m_samples->count);
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
                onResize(m_samples->count);
                slast = first;
                first = m_samples->first;
            }
            auto ipos = 0;
            auto pos = (info.samples->first - first) / interval;
            first = info.samples->first;
            for (; first < ilast; first += interval, ++pos, ++ipos) {
                auto & sval = m_samples->samples[pos];
                auto ival = info.samples->samples[ipos];
                onAggregate(sval, (int) pos, ival);
            }
            assert(pos <= m_samples->count);
        } else {
            // TODO: normalize and consolidate incompatible lists
            logMsgError() << "Aggregating incompatible series, "
                << info.name.get();
        }
        return true;
    }

    onFinalize();

    // Output aggregated result and end mark
    info.name = addFuncName(this->type(), info.target);
    info.samples = move(m_samples);
    notify->onFuncOutput(info);
    info.name = {};
    info.samples = {};
    notify->onFuncOutput(info);
    return true;
}

//===========================================================================
// averageSeries
//===========================================================================
namespace {
class FuncAverageSeries : public IAggregateBase<FuncAverageSeries> {
    void onResize(int count) override;
    void onAggregate(double & agg, int pos, double newVal) override;
    vector<unsigned> m_counts;
};
} // namespace
static auto s_averageSeries =
    FuncAverageSeries::Factory("averageSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true)
    .alias("avg");

//===========================================================================
void FuncAverageSeries::onResize(int count) {
    m_counts.resize(count, 1);
}

//===========================================================================
void FuncAverageSeries::onAggregate(double & agg, int pos, double newVal) {
    if (!isnan(newVal)) {
        auto cnt = m_counts[pos]++;
        agg = (agg * (cnt - 1) + newVal) / cnt;
    }
}

//===========================================================================
// countSeries
//===========================================================================
namespace {
class FuncCountSeries : public IAggregateBase<FuncCountSeries> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    void onAggregate(double & agg, int pos, double newVal) override;
    void onFinalize() override;
    unsigned m_count{0};
};
} // namespace
static auto s_countSeries =
    FuncCountSeries::Factory("countSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
bool FuncCountSeries::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples)
        m_count += 1;
    return impl_type::onFuncApply(notify, info);
}

//===========================================================================
void FuncCountSeries::onAggregate(double & agg, int pos, double newVal)
{}

//===========================================================================
void FuncCountSeries::onFinalize() {
    auto * ptr = m_samples->samples,
        * term = ptr + m_samples->count;
    for (; ptr != term; ++ptr) {
        *ptr = m_count;
    }
}

//===========================================================================
// diffSeries
//===========================================================================
namespace {
class FuncDiffSeries : public IAggregateBase<FuncDiffSeries> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    void onAggregate(double & agg, int pos, double newVal) override;
    unsigned m_count{0};
};
} // namespace
static auto s_diffSeries = FuncDiffSeries::Factory("diffSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
bool FuncDiffSeries::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    m_count += 1;
    return impl_type::onFuncApply(notify, info);
}

//===========================================================================
void FuncDiffSeries::onAggregate(double & agg, int pos, double newVal) {
    if (m_count == 1) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg -= newVal;
    }
}

//===========================================================================
// maxSeries
//===========================================================================
namespace {
class FuncMaxSeries : public IAggregateBase<FuncMaxSeries> {
    void onAggregate(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_maxSeries = FuncMaxSeries::Factory("maxSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMaxSeries::onAggregate(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal > agg)
        agg = newVal;
}

//===========================================================================
// minSeries
//===========================================================================
namespace {
class FuncMinSeries : public IAggregateBase<FuncMinSeries> {
    void onAggregate(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_minSeries = FuncMinSeries::Factory("minSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMinSeries::onAggregate(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal < agg)
        agg = newVal;
}

//===========================================================================
// multiplySeries
//===========================================================================
namespace {
class FuncMultiplySeries : public IAggregateBase<FuncMultiplySeries> {
    void onAggregate(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_multiplySeries =
    FuncMultiplySeries::Factory("multiplySeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true);

//===========================================================================
void FuncMultiplySeries::onAggregate(double & agg, int pos, double newVal) {
    if (isnan(agg)) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg *= newVal;
    }
}

//===========================================================================
// stddevSeries
//===========================================================================
namespace {
class FuncStddevSeries : public IAggregateBase<FuncStddevSeries> {
    void onResize(int count) override;
    void onAggregate(double & agg, int pos, double newVal) override;
    void onFinalize() override;
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
void FuncStddevSeries::onResize(int count) {
    auto base = (int) m_infos.size();
    m_infos.resize(count);
    for (int pos = base; pos < count; ++pos) {
        auto & agg = m_samples->samples[pos];
        auto & info = m_infos[pos];
        if (!isnan(agg)) {
            info.count = 1;
            info.mean = agg;
            agg = 0;
        }
    }
}

//===========================================================================
void FuncStddevSeries::onAggregate(double & agg, int pos, double newVal) {
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
void FuncStddevSeries::onFinalize() {
    int pos = 0;
    for (auto && info : m_infos) {
        auto & agg = m_samples->samples[pos++];
        agg = sqrt(agg / info.count);
    }
}

//===========================================================================
// sumSeries
//===========================================================================
namespace {
class FuncSumSeries : public IAggregateBase<FuncSumSeries> {
    void onAggregate(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_sumSeries = FuncSumSeries::Factory("sumSeries", "Combine")
    .arg("query", FuncArgInfo::kQuery, true, true)
    .alias("sum");

//===========================================================================
void FuncSumSeries::onAggregate(double & agg, int pos, double newVal) {
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

