// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// func.h - tismet func
#pragma once

#include "core/core.h"
#include "querydefs/querydefs.h"

#include <memory>
#include <string_view>
#include <vector>

// fnenum.h is a generated file that defines values for the Function::Type
// and Aggregate::Type enums. It is optional here so it can be generated
// without having to exist first.
#if __has_include("fnenum.h")
    #include "fnenum.h"
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
namespace AggFunc {
    enum Type : int;
    Type defaultType();
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
    static std::shared_ptr<SampleList> alloc(SampleList const & samples);
    static std::shared_ptr<SampleList> dup(SampleList const & samples);
};
inline double * begin(SampleList & samples) { return samples.samples; }
inline double * end(SampleList & samples) {
    return samples.samples + samples.count;
}

struct ResultInfo {
    std::shared_ptr<char[]> target;
    std::shared_ptr<char[]> name;
    std::shared_ptr<SampleList> samples;
    AggFunc::Type method{};
    int argPos{-1};
};

namespace FuncArg {
    struct Enum : Dim::ListBaseLink<> {
        std::string const name;
        Dim::TokenTable const * const table{};

        Enum(std::string name, Dim::TokenTable const * tbl);
    };
    enum Type {
        kEnum,
        kFunc,
        kNum,
        kNumOrString,
        kPath,
        kPathOrFunc,
        kString,
    };
    enum EnumType : int;
};

class IFuncNotify {
public:
    virtual ~IFuncNotify() = default;
    virtual bool onFuncSource(Query::Node const & node) = 0;
    virtual void onFuncOutput(ResultInfo & info) = 0;
};

struct FuncContext {
    int argPos{-1};
    Dim::TimePoint first;
    Dim::TimePoint last;

    Dim::Duration minInterval{};
    AggFunc::Type method{};

    // "pre" is a request for samples from before the start of the result
    // range that are needed to make the first values meaningful. These are
    // requested by functions such as movingAverage and derivative.
    Dim::Duration pretime{};
    unsigned presamples{0};
};

class IFuncInstance {
public:
    virtual ~IFuncInstance() = default;

    virtual Function::Type type() const = 0;

    // Validate and/or process arguments, and return the function instance to
    // use. Usually returns this or nullptr for errors, but may return an
    // entirely new function. For exaample aggregate(A, 'max') might bind into
    // maxSeries(A).
    virtual IFuncInstance * onFuncBind(
        IFuncNotify * notify,
        std::vector<Query::Node const *> & args
    ) = 0;

    virtual void onFuncAdjustContext(FuncContext * rr) = 0;

    // Perform the function, notify->onFuncOutput() must be called for each
    // result that should be published.
    //
    // Returns false to stop receiving results for the current outputs, only
    // required if the function is to be aborted midstream.
    virtual bool onFuncApply(IFuncNotify * notify, ResultInfo & info) = 0;
};

IFuncInstance * bind(
    IFuncNotify * notify,
    Function::Type type,
    std::vector<Query::Node const *> & args
);
std::shared_ptr<SampleList> reduce(
    std::shared_ptr<SampleList> samples,
    Dim::Duration minInterval,
    AggFunc::Type method = {} // use defaultType()
);

using AggFn = double(double const vals[], size_t count);
AggFn * aggFunc(
    AggFunc::Type method = {} // use defaultType()
);
double aggAverage(double const vals[], size_t count);
double aggCount(double const vals[], size_t count);
double aggDiff(double const vals[], size_t count);
double aggFirst(double const vals[], size_t count);
double aggLast(double const vals[], size_t count);
double aggMax(double const vals[], size_t count);
double aggMedian(double const vals[], size_t count);
double aggMin(double const vals[], size_t count);
double aggMultiply(double const vals[], size_t count);
double aggRange(double const vals[], size_t count);
double aggStddev(double const vals[], size_t count);
double aggSum(double const vals[], size_t count);

} // namespace

void funcInitialize();

std::unique_ptr<Eval::IFuncInstance> funcCreate(Eval::Function::Type type);

char const * toString(Eval::Function::Type ftype, char const def[] = "");
Eval::Function::Type fromString(std::string_view src, Eval::Function::Type def);

char const * toString(Eval::AggFunc::Type ftype, char const def[] = "");
Eval::AggFunc::Type fromString(
    std::string_view src,
    Eval::AggFunc::Type def
);


/****************************************************************************
*
*   Function factories
*
***/

namespace Eval {

struct FuncArgInfo {
    std::string name;
    FuncArg::Type type{};
    char const * enumName{};
    bool require{};
    bool multiple{};
};

class IFuncFactory
    : public Dim::ListBaseLink<>
    , public Dim::IFactory<IFuncInstance> {
public:
    IFuncFactory(std::string_view name, std::string_view group);
    IFuncFactory(IFuncFactory const & from);
    IFuncFactory(IFuncFactory && from);

    // Inherited via IFactory
    std::unique_ptr<IFuncInstance> onFactoryCreate() override = 0;

    Function::Type m_type{};
    std::vector<std::string> m_names;
    std::string m_group;
    std::vector<FuncArgInfo> m_args;
};

} // namespace

Query::ITokenConvNotify const & funcTokenConv();
Dim::List<Eval::IFuncFactory> & funcFactories();
Dim::List<Eval::FuncArg::Enum> & funcEnums();

char const * toString(Eval::FuncArg::Type atype, char const def[] = "");
