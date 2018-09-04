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
*   Private
*
***/

namespace {
using OperFn = bool(double a, double b);
struct OperatorInfo {
    OperFn * fn;
    vector<const char *> names;
};
} // namespace
static OperatorInfo s_opers[] = {
    { nullptr, { "" } },
    { [](auto a, auto b) { return a == b; }, { "eq", "=" } },
    { [](auto a, auto b) { return a != b; }, { "ne", "!=", "<>" }, },
    { [](auto a, auto b) { return a > b; },  { "gt", ">" }, },
    { [](auto a, auto b) { return a >= b; }, { "ge", ">=" }, },
    { [](auto a, auto b) { return a < b; },  { "lt", "<" }, },
    { [](auto a, auto b) { return a <= b; }, { "le", "<=" }, },
};
static vector<TokenTable::Token> s_operTokens;
const TokenTable s_operTbl = [](){
    for (unsigned i = 0; i < size(s_opers); ++i) {
        auto & v = s_opers[i];
        if (v.fn) {
            for (auto && n : v.names) {
                auto & token = s_operTokens.emplace_back();
                token.id = i;
                token.name = n;
            }
        }
    }
    return TokenTable{s_operTokens};
}();
const FuncArg::Enum s_operEnum{"operator", &s_operTbl};


/****************************************************************************
*
*   IFilterBase
*
***/

namespace {

template<typename T>
class IFilterBase : public IFuncBase<T> {
public:
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    virtual bool onFilter(const ResultInfo & info);
protected:
    double m_limit{};
    AggFn * m_aggFn;
    OperFn * m_operFn;
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
template<typename T>
bool IFilterBase<T>::onFilter(const ResultInfo & info) {
    auto agg = m_aggFn(info.samples->samples, info.samples->count);
    return m_operFn(agg, m_limit);
}


/****************************************************************************
*
*   filterSeries
*
***/

namespace {
class FilterSeries : public IFilterBase<FilterSeries> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
};
} // namespace
static auto s_filterSeries =
    FilterSeries::Factory("filterSeries", "Filter Series")
    .arg("query", FuncArg::kQuery, true)
    .arg("func", "aggFunc", true)
    .arg("operator", "operator", true)
    .arg("threshold", FuncArg::kNum, true);

//===========================================================================
IFuncInstance * FilterSeries::onFuncBind(vector<FuncArg> && args) {
    m_aggFn = aggFunc((Aggregate::Type)(int) args[0].number);
    m_operFn = s_opers[(int) args[1].number].fn;
    m_limit = args[2].number;
    return this;
}


/****************************************************************************
*
*   Delegate to filterSeries
*
***/

//===========================================================================
namespace {
template<int Agg, int Op>
class FilterSeriesBase : public IFilterBase<FilterSeriesBase<Agg,Op>> {
public:
    class Factory : public FuncFactory<FilterSeriesBase> {
    public:
        Factory(string_view name)
            : FuncFactory<FilterSeriesBase>(name, "Filter Series")
        {
            this->arg("query", FuncArg::kQuery, true);
            this->arg("n", FuncArg::kNum, true);
        }
    };
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override {
        this->m_aggFn = aggFunc((Aggregate::Type) Agg);
        this->m_operFn = s_opers[Op].fn;
        this->m_limit = args[0].number;
        return this;
    }
};
} // namespace

static auto s_averageAbove =
    FilterSeriesBase<AggFunc::kAverage, Operator::kGt>::Factory("averageAbove");
static auto s_averageBelow =
    FilterSeriesBase<AggFunc::kAverage, Operator::kLt>::Factory("averageBelow");
static auto s_currentAbove =
    FilterSeriesBase<AggFunc::kLast, Operator::kGt>::Factory("currentAbove");
static auto s_currentBelow =
    FilterSeriesBase<AggFunc::kLast, Operator::kLt>::Factory("currentBelow");
static auto s_maximumAbove =
    FilterSeriesBase<AggFunc::kMax, Operator::kGt>::Factory("maximumAbove");
static auto s_maximumBelow =
    FilterSeriesBase<AggFunc::kMax, Operator::kLt>::Factory("maximumBelow");
static auto s_minimumAbove =
    FilterSeriesBase<AggFunc::kMin, Operator::kGt>::Factory("minimumAbove");
static auto s_minimumBelow =
    FilterSeriesBase<AggFunc::kMin, Operator::kLt>::Factory("minimumBelow");


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
    .arg("query", FuncArg::kQuery, true)
    .arg("n", FuncArg::kNum, true);

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
    .arg("query", FuncArg::kQuery, true)
    .arg("n", FuncArg::kNum, true);

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
