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
    .arg("query", FuncArg::kQuery, true);

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
    .arg("query", FuncArg::kQuery, true);

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
*   removeAboveValue
*
***/

namespace {
class FuncRemoveAboveValue : public IXfrmValueBase<FuncRemoveAboveValue> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeAboveValue =
    FuncRemoveAboveValue::Factory("removeAboveValue", "Filter Data")
    .arg("query", FuncArg::kQuery, true)
    .arg("n", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveAboveValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
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
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_limit{};
};
} // namespace
static auto s_removeBelowValue =
    FuncRemoveBelowValue::Factory("removeBelowValue", "Filter Data")
    .arg("query", FuncArg::kQuery, true)
    .arg("n", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncRemoveBelowValue::onFuncBind(vector<FuncArg> && args) {
    m_limit = args[0].number;
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
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    double m_factor;
};
} // namespace
static auto s_scale = FuncScale::Factory("scale", "Transform")
    .arg("query", FuncArg::kQuery, true)
    .arg("factor", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FuncScale::onFuncBind(vector<FuncArg> && args) {
    m_factor = args[0].number;
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
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    double onConvert(double value) override;
    void onConvertStart(Duration interval) override;

    double m_seconds;
    double m_factor;
};
} // namespace
static auto s_scaleToSeconds =
    FuncScaleToSeconds::Factory("scaleToSeconds", "Transform")
    .arg("query", FuncArg::kQuery, true)
    .arg("seconds", FuncArg::kNum, true);

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
*   Public API
*
***/

//===========================================================================
void funcXfrmValueInitialize()
{}
