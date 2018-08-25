// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// func.h - tismet func
#pragma once

#include "core/time.h"

#include <memory>
#include <string_view>
#include <vector>

// funcenum.h is a generated file that defines values for the Function::Type
// and Aggregate::Type enums. It is optional here so it can be generated
// without having to exist first.
#if __has_include("funcenum.h")
    #include "funcenum.h"
#endif


/****************************************************************************
*
*   Functions
*
***/

namespace Eval {

namespace Function {
    enum Type : int;
}
namespace Aggregate {
    enum Type : int;
}

struct SampleList {
    Dim::TimePoint first;
    Dim::Duration interval{};
    unsigned count{0};
    uint32_t metricId{0};

    // EXTENDS BEYOND END OF STRUCT
    double samples[1];

    static std::shared_ptr<SampleList> alloc(
        Dim::TimePoint first,
        Dim::Duration interval,
        size_t count
    );
    static std::shared_ptr<SampleList> alloc(const SampleList & samples);
    static std::shared_ptr<SampleList> dup(const SampleList & samples);
};
inline double * begin(SampleList & samples) { return samples.samples; }
inline double * end(SampleList & samples) {
    return samples.samples + samples.count;
}

struct ResultInfo {
    std::shared_ptr<char[]> target;
    std::shared_ptr<char[]> name;
    std::shared_ptr<SampleList> samples;
    Aggregate::Type method{};
    int argPos{-1};
};

struct FuncArg {
    std::shared_ptr<char[]> string;
    double number;
};

class IFuncNotify {
public:
    virtual ~IFuncNotify() = default;
    virtual void onFuncOutput(ResultInfo & info) = 0;
};

class IFuncInstance {
public:
    virtual ~IFuncInstance() = default;

    virtual Function::Type type() const = 0;

    // Validate and/or process arguments, and return the function instance to
    // use. Usually returns this or nullptr for errors, but may return an
    // entirely new function. For exaample aggregate(A, 'max') might bind into
    // maxSeries(A).
    virtual IFuncInstance * onFuncBind(std::vector<FuncArg> && args) = 0;

    virtual void onFuncAdjustRange(
        Dim::TimePoint * first,
        Dim::TimePoint * last,
        Dim::Duration * pretime,
        unsigned * presamples
    ) = 0;

    // Perform the function, outputResult() must be called for each result
    // that should be published.
    //
    // Return false to stop receiving results for the current outputs, only
    // required if the function is to be aborted midstream.
    virtual bool onFuncApply(IFuncNotify * notify, ResultInfo & info) = 0;
};


std::shared_ptr<SampleList> reduce(
    std::shared_ptr<SampleList> samples,
    Dim::Duration minInterval,
    Aggregate::Type method
);

double aggAverage(const double vals[], size_t count);
double aggCount(const double vals[], size_t count);
double aggDiff(const double vals[], size_t count);
double aggLast(const double vals[], size_t count);
double aggMax(const double vals[], size_t count);
double aggMedian(const double vals[], size_t count);
double aggMin(const double vals[], size_t count);
double aggMultiply(const double vals[], size_t count);
double aggRange(const double vals[], size_t count);
double aggStddev(const double vals[], size_t count);
double aggSum(const double vals[], size_t count);

} // namespace

std::unique_ptr<Eval::IFuncInstance> funcCreate(Eval::Function::Type type);

const char * toString(Eval::Function::Type ftype, const char def[] = "");
Eval::Function::Type fromString(std::string_view src, Eval::Function::Type def);

const char * toString(Eval::Aggregate::Type ftype, const char def[] = "");
Eval::Aggregate::Type fromString(
    std::string_view src,
    Eval::Aggregate::Type def
);
