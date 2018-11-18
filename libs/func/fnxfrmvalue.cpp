// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// fnxfrmvalue.cpp - tismet func
//
// Changes to the individual samples in lists

#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;


/****************************************************************************
*
*   IXfrmValueBase
*
***/

namespace {
template<typename T>
class IXfrmValueBase : public IFuncBase<T> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual double onConvert(double value) = 0;
    virtual void onConvertStart(Duration interval) {}
};
} // namespace

//===========================================================================
template<typename T>
bool IXfrmValueBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
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


/****************************************************************************
*
*   absolute
*
***/

namespace {
class FuncAbsolute : public IXfrmValueBase<FuncAbsolute> {
    double onConvert(double value) override;
};
} // namespace
static auto s_absolute = FuncAbsolute::Factory("absolute", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

//===========================================================================
double FuncAbsolute::onConvert(double value) {
    return abs(value);
}


/****************************************************************************
*
*   drawAsInfinite
*
***/

namespace {
class FuncDrawAsInfinite : public IXfrmValueBase<FuncDrawAsInfinite> {
    double onConvert(double value) override;
};
} // namespace
static auto s_drawAsInfinite =
    FuncDrawAsInfinite::Factory("drawAsInfinite", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

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


/****************************************************************************
*
*   invert
*
***/

namespace {
class FuncInvert : public IXfrmValueBase<FuncInvert> {
    double onConvert(double value) override;
};
} // namespace
static auto s_invert = FuncInvert::Factory("invert", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

//===========================================================================
double FuncInvert::onConvert(double value) {
    return 1 / value;
}


/****************************************************************************
*
*   isNonNull
*
***/

namespace {
class FuncIsNonNull : public IXfrmValueBase<FuncIsNonNull> {
    double onConvert(double value) override;
};
} // namespace
static auto s_isNonNull = FuncIsNonNull::Factory("isNonNull", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

//===========================================================================
double FuncIsNonNull::onConvert(double value) {
    return isnan(value) ? 0 : 1;
}


/****************************************************************************
*
*   logarithm
*
***/

namespace {
class FuncLogarithm : public IXfrmValueBase<FuncLogarithm> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_base{log(10)};
};
} // namespace
static auto s_logarithm = FuncLogarithm::Factory("logarithm", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("base", FuncArg::kNum, false);

//===========================================================================
IFuncInstance * FuncLogarithm::onFuncBind(
    vector<Query::Node const *> & args
) {
    if (!args.empty())
        m_base = log(asNumber(*args[0]));
    return this;
}

//===========================================================================
double FuncLogarithm::onConvert(double value) {
    return log(value) / m_base;
}


/****************************************************************************
*
*   offset
*
***/

namespace {
class FuncOffset : public IXfrmValueBase<FuncOffset> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_factor{};
};
} // namespace
static auto s_offset = FuncOffset::Factory("offset", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("factor", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncOffset::onFuncBind(
    vector<Query::Node const *> & args
) {
    m_factor = asNumber(*args[0]);
    return this;
}

//===========================================================================
double FuncOffset::onConvert(double value) {
    return value + m_factor;
}


/****************************************************************************
*
*   pow
*
***/

namespace {
class FuncPow : public IXfrmValueBase<FuncPow> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_factor{};
};
} // namespace
static auto s_pow = FuncPow::Factory("pow", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("factor", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncPow::onFuncBind(
    vector<Query::Node const *> & args
) {
    m_factor = asNumber(*args[0]);
    return this;
}

//===========================================================================
double FuncPow::onConvert(double value) {
    return pow(value, m_factor);
}


/****************************************************************************
*
*   removeAboveValue
*
***/

namespace {
class FuncRemoveAboveValue : public IXfrmValueBase<FuncRemoveAboveValue> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeAboveValue =
    FuncRemoveAboveValue::Factory("removeAboveValue", "Filter Data")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("n", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveAboveValue::onFuncBind(
    vector<Query::Node const *> & args
) {
    m_limit = asNumber(*args[0]);
    return this;
}

//===========================================================================
double FuncRemoveAboveValue::onConvert(double value) {
    return value > m_limit ? NAN : value;
}


/****************************************************************************
*
*   removeBelowValue
*
***/

namespace {
class FuncRemoveBelowValue : public IXfrmValueBase<FuncRemoveBelowValue> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeBelowValue =
    FuncRemoveBelowValue::Factory("removeBelowValue", "Filter Data")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("n", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveBelowValue::onFuncBind(vector<Query::Node const *> & args) {
    m_limit = asNumber(*args[0]);
    return this;
}

//===========================================================================
double FuncRemoveBelowValue::onConvert(double value) {
    return value < m_limit ? NAN : value;
}


/****************************************************************************
*
*   scale
*
***/

namespace {
class FuncScale : public IXfrmValueBase<FuncScale> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    double m_factor;
};
} // namespace
static auto s_scale = FuncScale::Factory("scale", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("factor", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncScale::onFuncBind(vector<Query::Node const *> & args) {
    m_factor = asNumber(*args[0]);
    return this;
}

//===========================================================================
double FuncScale::onConvert(double value) {
    return value * m_factor;
}


/****************************************************************************
*
*   scaleToSeconds
*
***/

namespace {
class FuncScaleToSeconds : public IXfrmValueBase<FuncScaleToSeconds> {
    IFuncInstance * onFuncBind(vector<Query::Node const *> & args) override;
    double onConvert(double value) override;
    void onConvertStart(Duration interval) override;

    double m_seconds;
    double m_factor;
};
} // namespace
static auto s_scaleToSeconds =
    FuncScaleToSeconds::Factory("scaleToSeconds", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("seconds", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncScaleToSeconds::onFuncBind(vector<Query::Node const *> & args) {
    m_seconds = asNumber(*args[0]);
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
*   squareRoot
*
***/

namespace {
class FuncSquareRoot : public IXfrmValueBase<FuncSquareRoot> {
    double onConvert(double value) override;
};
} // namespace
static auto s_squareRoot = FuncSquareRoot::Factory("squareRoot", "Transform")
    .arg("query", FuncArg::kPathOrFunc, true);

//===========================================================================
double FuncSquareRoot::onConvert(double value) {
    return sqrt(value);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void funcXfrmValueInitialize()
{}
