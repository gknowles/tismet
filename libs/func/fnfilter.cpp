// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// fnfilter.cpp - tismet func
//
// Functions that filter out sample lists from results

#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;


/****************************************************************************
*
*   IFilterBase
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


/****************************************************************************
*
*   maximumAbove
*
***/

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
*   IFilterBestBase
*
***/

namespace {

template<typename T>
class IFilterBestBase : public IFuncBase<T> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    virtual double onFilterScore(SampleList & samples) = 0;
    virtual void onFilterUpdate(double score, ResultInfo & info) = 0;

protected:
    multimap<double, ResultInfo> m_best;
    unsigned m_allowed;
};

} // namespace

//===========================================================================
template<typename T>
IFuncInstance * IFilterBestBase<T>::onFuncBind(vector<FuncArg> && args) {
    m_allowed = (unsigned) args[0].number;
    return this;
}

//===========================================================================
template<typename T>
bool IFilterBestBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    // last non-NAN sample in list
    if (!info.samples) {
        for (auto && out : m_best)
            notify->onFuncOutput(out.second);
        info.name = {};
        info.samples = {};
        notify->onFuncOutput(info);
        m_best.clear();
        return true;
    }

    auto best = onFilterScore(*info.samples);
    if (!isnan(best)) {
        if (m_best.size() < m_allowed) {
            m_best.emplace(best, info);
        } else if (m_allowed >= 0) {
            onFilterUpdate(best, info);
        }
    }
    return true;
}


/****************************************************************************
*
*   highestCurrent
*
***/

namespace {
class FuncHighestCurrent : public IFilterBestBase<FuncHighestCurrent> {
    double onFilterScore(SampleList & samples) override;
    void onFilterUpdate(double score, ResultInfo & info) override;
};
} // namespace
static auto s_highestCurrent =
    FuncHighestCurrent::Factory("highestCurrent", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
double FuncHighestCurrent::onFilterScore(SampleList & samples) {
    // last non-NAN sample in list
    for (int i = samples.count; i-- > 0;) {
        if (auto score = samples.samples[i]; !isnan(score))
            return score;
    }
    return NAN;
}

//===========================================================================
void FuncHighestCurrent::onFilterUpdate(double score, ResultInfo & info) {
    if (auto i = m_best.begin(); score > i->first) {
        m_best.erase(i);
        m_best.emplace(score, info);
    }
}


/****************************************************************************
*
*   highestMax
*
***/

namespace {
class FuncHighestMax : public IFilterBestBase<FuncHighestMax> {
    double onFilterScore(SampleList & samples) override;
    void onFilterUpdate(double score, ResultInfo & info) override;
};
} // namespace
static auto s_highestMax =
    FuncHighestMax::Factory("highestMax", "Filter Series")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("n", FuncArgInfo::kNum, true);

//===========================================================================
double FuncHighestMax::onFilterScore(SampleList & samples) {
    // largest non-NAN sample in list
    auto score = -numeric_limits<double>::infinity();
    bool found = false;
    for (auto && ref : samples) {
        if (ref > score) {
            score = ref;
            found = true;
        }
    }
    return found ? score : NAN;
}

//===========================================================================
void FuncHighestMax::onFilterUpdate(double score, ResultInfo & info) {
    if (auto i = m_best.begin(); score > i->first) {
        m_best.erase(i);
        m_best.emplace(score, info);
    }
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void funcFilterInitialize()
{}
