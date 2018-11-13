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
*   filterSeries
*
***/

namespace {

template<int Agg, int Op>
class FilterSeries : public IFuncBase<FilterSeries<Agg,Op>> {
public:
    class Factory;
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
protected:
    double m_limit{};
    AggFn * m_aggFn;
    OperFn * m_operFn;
};

template<int Agg, int Op>
class FilterSeries<Agg, Op>::Factory : public FuncFactory<FilterSeries> {
public:
    Factory(string_view name);
};

} // namespace

//===========================================================================
template<int Agg, int Op>
IFuncInstance * FilterSeries<Agg, Op>::onFuncBind(
    vector<FuncArg> && args
) {
    if constexpr (Agg != 0) {
        m_aggFn = aggFunc((AggFunc::Type) Agg);
        m_operFn = s_opers[Op].fn;
        m_limit = args[0].number;
    } else {
        m_aggFn = aggFunc((AggFunc::Type)(int) args[0].number);
        m_operFn = s_opers[(int) args[1].number].fn;
        m_limit = args[2].number;
    }
    return this;
}

//===========================================================================
template<int Agg, int Op>
bool FilterSeries<Agg, Op>::onFuncApply(
    IFuncNotify * notify,
    ResultInfo & info
) {
    if (info.samples) {
        auto agg = m_aggFn(info.samples->samples, info.samples->count);
        if (!m_operFn(agg, m_limit))
            return true;
    }
    notify->onFuncOutput(info);
    return true;
}

//===========================================================================
template<int Agg, int Op>
FilterSeries<Agg, Op>::Factory::Factory(string_view name)
    : FuncFactory<FilterSeries>(name, "Filter Series")
{
    this->arg("query", FuncArg::kPathOrFunc, true);
    if constexpr (Agg != 0) {
        this->arg("n", FuncArg::kNum, true);
    } else {
        this->arg("func", "aggFunc", true);
        this->arg("operator", "operator", true);
        this->arg("threshold", FuncArg::kNum, true);
    }
}

static auto s_filterSeries =
    FilterSeries<0, 0>::Factory("filterSeries");
static auto s_averageAbove =
    FilterSeries<AggFunc::kAverage, Operator::kGt>::Factory("averageAbove");
static auto s_averageBelow =
    FilterSeries<AggFunc::kAverage, Operator::kLt>::Factory("averageBelow");
static auto s_currentAbove =
    FilterSeries<AggFunc::kLast, Operator::kGt>::Factory("currentAbove");
static auto s_currentBelow =
    FilterSeries<AggFunc::kLast, Operator::kLt>::Factory("currentBelow");
static auto s_maximumAbove =
    FilterSeries<AggFunc::kMax, Operator::kGt>::Factory("maximumAbove");
static auto s_maximumBelow =
    FilterSeries<AggFunc::kMax, Operator::kLt>::Factory("maximumBelow");
static auto s_minimumAbove =
    FilterSeries<AggFunc::kMin, Operator::kGt>::Factory("minimumAbove");
static auto s_minimumBelow =
    FilterSeries<AggFunc::kMin, Operator::kLt>::Factory("minimumBelow");


/****************************************************************************
*
*   FilterBest
*
***/

namespace {

template<int Agg, int Op>
class FilterBest : public IFuncBase<FilterBest<Agg,Op>> {
public:
    class Factory;
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
protected:
    multimap<double, ResultInfo> m_best;
    unsigned m_allowed{0};
    AggFn * m_aggFn{nullptr};
};

template<int Agg, int Op>
class FilterBest<Agg, Op>::Factory : public FuncFactory<FilterBest> {
public:
    Factory(string_view name);
};

} // namespace

//===========================================================================
template<int Agg, int Op>
IFuncInstance * FilterBest<Agg, Op>::onFuncBind(
    vector<FuncArg> && args
) {
    if constexpr (Agg != 0) {
        m_aggFn = aggFunc((AggFunc::Type) Agg);
        m_allowed = (unsigned) args[0].number;
    } else {
        if (args.size() == 2) {
            m_allowed = (unsigned) args[0].number;
            auto afn = fromString(args[1].string.get(), AggFunc::kInvalid);
            m_aggFn = aggFunc(afn);
        } else if (args.size() == 1) {
            m_allowed = (unsigned) args[0].number;
            m_aggFn = aggFunc((AggFunc::Type)(int) args[1].number);
        } else {
            m_allowed = 1;
            m_aggFn = aggFunc(AggFunc::kAverage);
        }
    }
    return this;
}

//===========================================================================
template<int Agg, int Op>
bool FilterBest<Agg, Op>::onFuncApply(
    IFuncNotify * notify,
    ResultInfo & info
) {
    if (!info.samples) {
        for (auto && out : m_best)
            notify->onFuncOutput(out.second);
        info.name = {};
        info.samples = {};
        notify->onFuncOutput(info);
        m_best.clear();
        return true;
    }

    auto best = m_aggFn(info.samples->samples, info.samples->count);
    if (!isnan(best)) {
        if (m_best.size() < m_allowed) {
            m_best.emplace(best, info);
        } else if (m_allowed > 0) {
            if constexpr (Op == Operator::kLt) {
                if (auto i = m_best.end()--; best < i->first) {
                    m_best.erase(i);
                    m_best.emplace(best, info);
                }
            } else {
                if (auto i = m_best.begin(); best > i->first) {
                    m_best.erase(i);
                    m_best.emplace(best, info);
                }
            }
        }
    }
    return true;
}

//===========================================================================
template<int Agg, int Op>
FilterBest<Agg, Op>::Factory::Factory(string_view name)
    : FuncFactory<FilterBest>(name, "Filter Series")
{
    this->arg("query", FuncArg::kPathOrFunc, true);
    if constexpr (Agg != 0) {
        this->arg("n", FuncArg::kNum, true);
    } else {
        this->arg("n", FuncArg::kNum);
        this->arg("func", "aggFunc");
    }
}

static auto s_highest =
    FilterBest<0, Operator::kGt>::Factory("highest");
static auto s_highestAverage =
    FilterBest<AggFunc::kAverage, Operator::kGt>::Factory("highestAverage");
static auto s_highestCurrent =
    FilterBest<AggFunc::kLast, Operator::kGt>::Factory("highestCurrent");
static auto s_highestMax =
    FilterBest<AggFunc::kMax, Operator::kGt>::Factory("highestMax");
static auto s_lowest =
    FilterBest<0, Operator::kLt>::Factory("lowest");
static auto s_lowestAverage =
    FilterBest<AggFunc::kAverage, Operator::kLt>::Factory("lowestAverage");
static auto s_lowestCurrent =
    FilterBest<AggFunc::kLast, Operator::kLt>::Factory("lowestCurrent");


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void funcFilterInitialize()
{}
