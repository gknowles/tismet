// Copyright Glen Knowles 2018 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// fnxfrmlist.cpp - tismet func
//
// Functions that make changes within a single sample list

#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;


/****************************************************************************
*
*   IXfrmListBase
*
***/

namespace {
template<typename T>
class IXfrmListBase : public IFuncBase<T> {
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
bool IXfrmListBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
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


/****************************************************************************
*
*   derivative
*
***/

namespace {
class FuncDerivative : public IXfrmListBase<FuncDerivative> {
    IFuncInstance * onFuncBind(vector<const Query::Node *> & args) override;
    void onTransform(
        double * optr,
        const double * ptr,
        const double * eptr,
        Duration interval
    ) override;
};
} // namespace
static auto s_derivative = FuncDerivative::Factory("derivative", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

//===========================================================================
IFuncInstance * FuncDerivative::onFuncBind(
    vector<const Query::Node *> & args
) {
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


/****************************************************************************
*
*   keepLastValue
*
***/

namespace {
class FuncKeepLastValue : public IXfrmListBase<FuncKeepLastValue> {
    IFuncInstance * onFuncBind(vector<const Query::Node *> & args) override;
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
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("limit", FuncArg::kNum);

//===========================================================================
IFuncInstance * FuncKeepLastValue::onFuncBind(
    vector<const Query::Node *> & args
) {
    m_limit = args.empty() ? 0 : (int) asNumber(*args[0]);
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


/****************************************************************************
*
*   movingAverage
*
***/

namespace {
class FuncMovingAverage : public IXfrmListBase<FuncMovingAverage> {
    IFuncInstance * onFuncBind(vector<const Query::Node *> & args) override;
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
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("windowSize", FuncArg::kNumOrString, true)
    .arg("xFilesFactor", FuncArg::kNum, false);

//===========================================================================
IFuncInstance * FuncMovingAverage::onFuncBind(
    vector<const Query::Node *> & args
) {
    if (auto arg0 = asString(*args[0]); !arg0.empty()) {
        if (parse(&m_pretime, arg0))
            return this;
        m_presamples = strToUint(arg0);
    } else {
        m_presamples = (unsigned) asNumber(*args[0]);
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


/****************************************************************************
*
*   nonNegativeDerivative
*
***/

namespace {
class FuncNonNegativeDerivative
    : public IXfrmListBase<FuncNonNegativeDerivative>
{
    IFuncInstance * onFuncBind(vector<const Query::Node *> & args) override;
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
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("maxValue", FuncArg::kNum);

//===========================================================================
IFuncInstance * FuncNonNegativeDerivative::onFuncBind(
    vector<const Query::Node *> & args
) {
    m_limit = args.empty() ? HUGE_VAL : asNumber(*args[0]);
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
*   Public API
*
***/

//===========================================================================
void funcXfrmListInitialize()
{}
