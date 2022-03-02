// Copyright Glen Knowles 2018 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// fnagg.cpp - tismet func
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
double Eval::aggAverage(const double vals[], size_t count) {
    double out = 0;
    int cnt = 0;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (!isnan(*ptr)) {
            out += *ptr;
            cnt += 1;
        }
    }
    return cnt ? out / cnt : NAN;
}

//===========================================================================
double Eval::aggCount(const double vals[], size_t count) {
    return (double) count_if(vals, vals + count, [](auto & a) {
        return !isnan(a);
    });
}

//===========================================================================
double Eval::aggDiff(const double vals[], size_t count) {
    double out = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        out = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (!isnan(*ptr))
                out -= *ptr;
        }
        break;
    }
    return out;
}

//===========================================================================
double Eval::aggFirst(const double vals[], size_t count) {
    auto evals = vals + count;
    for (; vals < evals; ++vals) {
        if (!isnan(*vals))
            return *vals;
    }
    return NAN;
}

//===========================================================================
double Eval::aggLast(const double vals[], size_t count) {
    while (count--) {
        if (!isnan(vals[count]))
            return vals[count];
    }
    return NAN;
}

//===========================================================================
double Eval::aggMax(const double vals[], size_t count) {
    double out = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        out = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (*ptr > out)
                out = *ptr;
        }
        break;
    }
    return out;
}

//===========================================================================
double Eval::aggMedian(const double vals[], size_t count) {
    unique_ptr<double[]> tmp;
    double * nvals;
    if (count > 1000) {
        tmp = make_unique<double[]>(count);
        nvals = tmp.get();
    } else {
        nvals = (double *) alloca(count * sizeof(*nvals));
    }
    auto ncount = 0;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (!isnan(*ptr))
            nvals[ncount++] = *ptr;
    }
    if (!ncount)
        return NAN;
    sort(nvals, nvals + ncount);
    return (ncount % 2)
        ? nvals[ncount / 2]
        : (nvals[ncount / 2] + nvals[ncount / 2 - 1]) / 2;
}

//===========================================================================
double Eval::aggMin(const double vals[], size_t count) {
    double out = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        out = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (*ptr < out)
                out = *ptr;
        }
        break;
    }
    return out;
}

//===========================================================================
double Eval::aggMultiply(const double vals[], size_t count) {
    double out = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        out = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (!isnan(*ptr))
                out *= *ptr;
        }
        break;
    }
    return out;
}

//===========================================================================
double Eval::aggRange(const double vals[], size_t count) {
    double low = NAN;
    double high = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        low = high = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (*ptr < low)
                low = *ptr;
            else if (*ptr > high)
                high = *ptr;
        }
        break;
    }
    return high - low;
}

//===========================================================================
double Eval::aggStddev(const double vals[], size_t count) {
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        double omean = *ptr++;
        double agg = 0;
        unsigned cnt = 1;
        for (; ptr < vals + count; ++ptr) {
            if (isnan(*ptr))
                continue;
            auto val = *ptr;
            cnt += 1;
            auto mean = omean + (val - omean) / cnt;
            agg += (val - omean) * (val - mean);
            omean = mean;
        }
        return sqrt(agg / cnt);
    }
    return NAN;
}

//===========================================================================
double Eval::aggSum(const double vals[], size_t count) {
    double out = NAN;
    for (auto ptr = vals; ptr < vals + count; ++ptr) {
        if (isnan(*ptr))
            continue;
        out = *ptr++;
        for (; ptr < vals + count; ++ptr) {
            if (!isnan(*ptr))
                out += *ptr;
        }
        break;
    }
    return out;
}

//===========================================================================
template<double Fn(const double vals[], size_t)>
static void reduce(
    double out[],
    size_t outLen,
    double in[],
    size_t inLen,
    size_t sps,
    size_t presamples
) {
    auto ptr = in;
    auto eptr = in + inLen;
    auto optr = out;
    auto num = min(sps - presamples, inLen);
    *optr++ = Fn(ptr, num);
    ptr += num;
    while (ptr + sps <= eptr) {
        *optr++ = Fn(ptr, sps);
        ptr += sps;
    }
    if (ptr < eptr)
        *optr++ = Fn(ptr, eptr - ptr);
    assert(optr == out + outLen);
}


/****************************************************************************
*
*   Private
*
***/

namespace {
struct MethodInfo {
    AggFn * aggFn;
    ReduceFn * fn;
    vector<const char *> names;
};
} // namespace
static MethodInfo s_methods[] = {
    { nullptr, nullptr, { "" } },
    { aggAverage,  reduce<aggAverage>,  { "average", "avg" } },
    { aggCount,    reduce<aggCount>,    { "count" } },
    { aggDiff,     reduce<aggDiff>,     { "diff" } },
    { aggFirst,    reduce<aggFirst>,    { "first" } },
    { aggLast,     reduce<aggLast>,     { "last", "current" } },
    { aggMax,      reduce<aggMax>,      { "max" } },
    { aggMedian,   reduce<aggMedian>,   { "median" } },
    { aggMin,      reduce<aggMin>,      { "min" } },
    { aggMultiply, reduce<aggMultiply>, { "multiply" } },
    { aggRange,    reduce<aggRange>,    { "range", "rangeOf" } },
    { aggStddev,   reduce<aggStddev>,   { "stddev" } },
    { aggSum,      reduce<aggSum>,      { "sum", "total" } },
};
static vector<TokenTable::Token> s_methodTokens;
const TokenTable s_methodTbl = [](){
    sort(begin(s_methods), end(s_methods), [](auto & a, auto & b) {
        return strcmp(a.names.front(), b.names.front()) < 0;
    });
    for (unsigned i = 0; i < size(s_methods); ++i) {
        auto & v = s_methods[i];
        if (v.fn) {
            for (auto && n : v.names) {
                auto & token = s_methodTokens.emplace_back();
                token.id = i;
                token.name = n;
            }
        }
    }
    return TokenTable{s_methodTokens};
}();

static FuncArg::Enum s_methodEnum{"aggFunc", &s_methodTbl};

const AggFunc::Type s_defMethod = fromString("average", AggFunc::Type{});


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
AggFunc::Type AggFunc::defaultType() {
    return s_defMethod;
}

//===========================================================================
const char * toString(AggFunc::Type ftype, const char def[]) {
    return s_methodTbl.findName(ftype, def);
}

//===========================================================================
AggFunc::Type fromString(std::string_view src, AggFunc::Type def) {
    return s_methodTbl.find(src, def);
}

//===========================================================================
shared_ptr<SampleList> Eval::reduce(
    shared_ptr<SampleList> samples,
    Duration minInterval,
    AggFunc::Type method
) {
    auto baseInterval = samples->interval;
    if (baseInterval >= minInterval)
        return samples;

    if (!method)
        method = AggFunc::defaultType();
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

//===========================================================================
Eval::AggFn * Eval::aggFunc(AggFunc::Type method) {
    if (!method)
        method = AggFunc::defaultType();
    return s_methods[(int) method].aggFn;
}
