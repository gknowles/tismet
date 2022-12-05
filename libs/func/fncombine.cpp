// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// fncombine.cpp - tismet func
//
// Functions that combined samples for a single time interval

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


/****************************************************************************
*
*   Rebinding function implementations
*
***/

namespace {
class FuncAggregate : public IFuncBase<FuncAggregate> {
    IFuncInstance * onFuncBind(
        IFuncNotify * notify,
        vector<const Query::Node *> & args
    ) override;
};
} // namespace
static auto s_aggregate = FuncAggregate::Factory("aggregate", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true)
    .arg("aggFunc", "aggFunc", true);

//===========================================================================
IFuncInstance * FuncAggregate::onFuncBind(
    IFuncNotify * notify,
    vector<const Query::Node *> & args
) {
    auto aggtype = fromString(asString(*args[1]), AggFunc::defaultType());
    auto fname = string(toString(aggtype, "")) + "Series";
    args.erase(args.begin() + 1);
    auto type = fromString(fname, Function::kSumSeries);
    return bind(notify, type, args);
}


/****************************************************************************
*
*   ICombineBase
*
***/

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


/****************************************************************************
*
*   averageSeries
*
***/

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
    .arg("query", FuncArg::kPathOrFunc, true, true)
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


/****************************************************************************
*
*   countSeries
*
***/

namespace {
class FuncCountSeries : public ICombineBase<FuncCountSeries> {
    void onFuncAdjustContext(FuncContext * context) override;
    void onCombineApply(
        ResultInfo & info,
        TimePoint last,
        TimePoint sfirst
    ) override;
    void onCombineFinalize() override;
    unsigned m_count{};
    TimePoint m_first;
    TimePoint m_last;
    Duration m_interval;
};
} // namespace
static auto s_countSeries =
    FuncCountSeries::Factory("countSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncCountSeries::onFuncAdjustContext(FuncContext * context) {
    impl_type::onFuncAdjustContext(context);

    m_interval = context->minInterval;
    if (!m_interval.count())
        m_interval = 1s;
    m_first = context->first
        - context->pretime
        - context->presamples * m_interval;
    m_first -= m_first.time_since_epoch() % m_interval;
    m_last = context->last + m_interval;
    m_last -= m_last.time_since_epoch() % m_interval;

    m_count = 1;
}

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
    if (!m_samples) {
        auto num = (m_last - m_first) / m_interval;
        m_samples = SampleList::alloc(m_first, m_interval, num);
        m_count = 0;
    }
    for (auto && ref : *m_samples)
        ref = m_count;
}


/****************************************************************************
*
*   diffSeries
*
***/

namespace {
class FuncDiffSeries : public ICombineBase<FuncDiffSeries> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    bool m_firstArg{false};
};
} // namespace
static auto s_diffSeries = FuncDiffSeries::Factory("diffSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

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


/****************************************************************************
*
*   firstSeries
*
***/

namespace {
class FuncFirstSeries : public ICombineBase<FuncFirstSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_firstSeries =
    FuncFirstSeries::Factory("firstSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncFirstSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg))
        agg = newVal;
}


/****************************************************************************
*
*   lastSeries
*
***/

namespace {
class FuncLastSeries : public ICombineBase<FuncLastSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_lastSeries =
    FuncLastSeries::Factory("lastSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncLastSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (!isnan(newVal))
        agg = newVal;
}


/****************************************************************************
*
*   maxSeries
*
***/

namespace {
class FuncMaxSeries : public ICombineBase<FuncMaxSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_maxSeries = FuncMaxSeries::Factory("maxSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncMaxSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal > agg)
        agg = newVal;
}


/****************************************************************************
*
*   medianSeries
*
***/

namespace {
class FuncMedianSeries : public ICombineBase<FuncMedianSeries> {
    void onCombineResize(int prefix, int postfix) override;
    void onCombineValue(double & agg, int pos, double newVal) override;
    void onCombineFinalize() override;
    void onCombineClear() override;
    vector<vector<double>> m_samplesByPos;
};
} // namespace
static auto s_medianSeries =
    FuncMedianSeries::Factory("medianSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

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


/****************************************************************************
*
*   minSeries
*
***/

namespace {
class FuncMinSeries : public ICombineBase<FuncMinSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_minSeries = FuncMinSeries::Factory("minSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncMinSeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg) || newVal < agg)
        agg = newVal;
}


/****************************************************************************
*
*   multiplySeries
*
***/

namespace {
class FuncMultiplySeries : public ICombineBase<FuncMultiplySeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_multiplySeries =
    FuncMultiplySeries::Factory("multiplySeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true);

//===========================================================================
void FuncMultiplySeries::onCombineValue(double & agg, int pos, double newVal) {
    if (isnan(agg)) {
        agg = newVal;
    } else if (!isnan(newVal)) {
        agg *= newVal;
    }
}


/****************************************************************************
*
*   rangeSeries
*
***/

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
    .arg("query", FuncArg::kPathOrFunc, true, true);

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


/****************************************************************************
*
*   stddevSeries
*
***/

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
    .arg("query", FuncArg::kPathOrFunc, true, true);

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


/****************************************************************************
*
*   sumSeries
*
***/

namespace {
class FuncSumSeries : public ICombineBase<FuncSumSeries> {
    void onCombineValue(double & agg, int pos, double newVal) override;
};
} // namespace
static auto s_sumSeries = FuncSumSeries::Factory("sumSeries", "Combine")
    .arg("query", FuncArg::kPathOrFunc, true, true)
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
*   Public API
*
***/

//===========================================================================
void funcCombineInitialize()
{}
