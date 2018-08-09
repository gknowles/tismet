// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// funcagg.cpp - tismet func
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;


/****************************************************************************
*
*   Reduce functions
*
***/

namespace {

// Every output sample is constructed from a fixed number of input samples
// equal to samplesPerSample. The input stream is considered to include a
// number of presamples before in[0] that are NANs. The presamples are for
// alignment of the input and the number of them is always less than samples
// per sample.
using ReduceFn = void (
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t samplesPerSample,
    size_t presamples
);

} // namespace

//===========================================================================
static void reduceAverage(
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t sps,
    size_t presamples
) {
    auto oval = 0.0;
    auto num = presamples;
    auto nans = presamples;
    auto optr = out;
    for (unsigned i = 0; i < inLen; ++i) {
        auto val = in[i];
        if (num == sps) {
            *optr++ = nans == num ? NAN : oval / (num - nans);
            num = 1;
            if (isnan(val)) {
                oval = 0;
                nans = 1;
            } else {
                oval = val;
                nans = 0;
            }
        } else {
            num += 1;
            if (isnan(val)) {
                nans += 1;
            } else {
                oval += val;
            }
        }
    }
    *optr++ = (nans == num) ? NAN : oval / (num - nans);
    assert(optr == out + outLen);
}

//===========================================================================
static void reduceCount(
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t sps,
    size_t presamples
) {
    auto oval = 0;
    auto num = presamples;
    auto optr = out;
    for (unsigned i = 0; i < inLen; ++i) {
        auto val = in[i];
        if (num == sps) {
            *optr++ = oval;
            num = 1;
            if (isnan(val)) {
                oval = 0;
            } else {
                oval = 1;
            }
        } else {
            num += 1;
            if (!isnan(val))
                oval += 1;
        }
    }
    *optr++ = oval;
    assert(optr == out + outLen);
}

//===========================================================================
static void reduceMax(
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t sps,
    size_t presamples
) {
    auto oval = (double) NAN;
    auto num = presamples;
    auto optr = out;
    for (unsigned i = 0; i < inLen; ++i) {
        auto val = in[i];
        if (num == sps) {
            *optr++ = oval;
            num = 1;
            oval = val;
        } else {
            num += 1;
            if (!(val <= oval) && !isnan(val))
                oval = val;
        }
    }
    *optr++ = oval;
    assert(optr == out + outLen);
}

//===========================================================================
static void reduceMin(
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t sps,
    size_t presamples
) {
    auto oval = (double) NAN;
    auto num = presamples;
    auto optr = out;
    for (unsigned i = 0; i < inLen; ++i) {
        auto val = in[i];
        if (num == sps) {
            *optr++ = oval;
            num = 1;
            oval = val;
        } else {
            num += 1;
            if (!(val >= oval) && !isnan(val))
                oval = val;
        }
    }
    *optr++ = oval;
    assert(optr == out + outLen);
}


/****************************************************************************
*
*   Private
*
***/

namespace {
struct MethodInfo {
    ReduceFn * fn;
    vector<const char *> names;
    Aggregate::Type type{};
};
} // namespace
static MethodInfo s_methods[] = {
    { reduceAverage, { "average", "avg" } },
    { reduceCount, { "count" } },
    { nullptr, { "diff" } },
    { nullptr, { "last", "current" } },
    { reduceMax, { "max" } },
    { nullptr, { "median" } },
    { reduceMin, { "min" } },
    { nullptr, { "multiply" } },
    { nullptr, { "range", "rangeOf" } },
    { nullptr, { "stddev" } },
    { nullptr, { "sum", "total" } },
};
static vector<TokenTable::Token> s_methodTokens;
static TokenTable s_methodTbl = [](){
    sort(begin(s_methods), end(s_methods), [](auto & a, auto & b) {
        return strcmp(a.names.front(), b.names.front()) < 0;
    });
    for (unsigned i = 0; i < size(s_methods); ++i) {
        auto & m = s_methods[i];
        if (m.fn) {
            for (auto && n : m.names) {
                auto & token = s_methodTokens.emplace_back();
                token.id = i;
                token.name = n;
            }
        }
    }
    return TokenTable{s_methodTokens.data(), s_methodTokens.size()};
}();


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
const TokenTable & funcAggEnums() {
    return s_methodTbl;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
const char * toString(Eval::Aggregate::Type ftype, const char def[]) {
    return tokenTableGetName(s_methodTbl, ftype, def);
}

//===========================================================================
Aggregate::Type fromString(std::string_view src, Eval::Aggregate::Type def) {
    return tokenTableGetEnum(s_methodTbl, src, def);
}

//===========================================================================
shared_ptr<SampleList> Eval::reduce(
    shared_ptr<SampleList> samples,
    Duration minInterval,
    Aggregate::Type method
) {
    auto baseInterval = samples->interval;
    if (baseInterval >= minInterval)
        return samples;

    auto methodFn = s_methods[(int) method].fn;
    auto sps = (minInterval.count() + baseInterval.count() - 1)
        / baseInterval.count();
    auto maxInterval = sps * baseInterval;
    auto first = samples->first;
    first -= first.time_since_epoch() % maxInterval;
    auto presamples = (samples->first - first) / baseInterval;
    auto count = samples->count;
    auto out = SampleList::alloc(
        first,
        maxInterval,
        (count + presamples + sps - 1) / sps
    );

    methodFn(
        out->samples,
        out->count,
        samples->samples,
        samples->count,
        sps,
        presamples
    );
    return out;
}
