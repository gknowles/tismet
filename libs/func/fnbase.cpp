// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// fnbase.cpp - tismet func
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


/****************************************************************************
*
*   Variables
*
***/

static List<IFuncFactory> s_factories;


/****************************************************************************
*
*   SampleList
*
***/

//===========================================================================
// static
shared_ptr<SampleList> SampleList::alloc(
    TimePoint first,
    Duration interval,
    size_t count
) {
    auto vptr = new char[
        offsetof(SampleList, samples) + count * sizeof(*SampleList::samples)
    ];
    auto list = new(vptr) SampleList{first, interval, (uint32_t) count};
    return shared_ptr<SampleList>(list);
}

//===========================================================================
// static
shared_ptr<SampleList> SampleList::alloc(const SampleList & samples) {
    return alloc(samples.first, samples.interval, samples.count);
}

//===========================================================================
// static
shared_ptr<SampleList> SampleList::dup(const SampleList & samples) {
    auto out = alloc(samples);
    memcpy(
        out->samples,
        samples.samples,
        out->count * sizeof(*out->samples)
    );
    return out;
}


/****************************************************************************
*
*   IFuncFactory
*
***/

//===========================================================================
IFuncFactory::IFuncFactory(string_view name, string_view group)
    : m_group{group}
{
    s_factories.link(this);
    m_names.push_back(string{name});
}

//===========================================================================
IFuncFactory::IFuncFactory(const IFuncFactory & from)
    : m_type(from.m_type)
    , m_names(from.m_names)
    , m_group(from.m_group)
    , m_args(from.m_args)
{
    s_factories.link(this);
}

//===========================================================================
IFuncFactory::IFuncFactory(IFuncFactory && from)
    : m_type(from.m_type)
    , m_names(move(from.m_names))
    , m_group(move(from.m_group))
    , m_args(move(from.m_args))
{
    s_factories.link(this);
}


/****************************************************************************
*
*   PassthruBase
*
***/

namespace {
class PassthruBase : public IFuncBase<PassthruBase> {
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;
};
} // namespace

//===========================================================================
bool PassthruBase::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    notify->onFuncOutput(info);
    return true;
}

static auto s_aliasSub = PassthruBase::Factory("aliasSub", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("search", FuncArgInfo::kString, true)
    .arg("replace", FuncArgInfo::kString, true);
static auto s_color = PassthruBase::Factory("color", "Graph")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("color", FuncArgInfo::kString, true);
static auto s_legendValue = PassthruBase::Factory("legendValue", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("valuesTypes", FuncArgInfo::kString, false, true);
static auto s_lineWidth = PassthruBase::Factory("lineWidth", "Graph")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("width", FuncArgInfo::kNum, true);


/****************************************************************************
*
*   FuncAlias
*
***/

namespace {
class FuncAlias : public IFuncBase<FuncAlias> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    shared_ptr<char[]> m_name;
};
} // namespace
static auto s_alias = FuncAlias::Factory("alias", "Alias")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("name", FuncArgInfo::kString, true);

//===========================================================================
IFuncInstance * FuncAlias::onFuncBind(vector<FuncArg> && args) {
    m_name = args[0].string;
    return this;
}

//===========================================================================
bool FuncAlias::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    info.name = m_name;
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   FuncConsolidateBy
*
***/

namespace {
class FuncConsolidateBy : public IFuncBase<FuncConsolidateBy> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    Aggregate::Type m_method;
};
} // namespace
static auto s_consolidateBy =
    FuncConsolidateBy::Factory("consolidateBy", "Special")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("method", FuncArgInfo::kAggFunc, true);

//===========================================================================
IFuncInstance * FuncConsolidateBy::onFuncBind(vector<FuncArg> && args) {
    m_method = fromString(args[0].string.get(), Aggregate::Type{});
    return this;
}

//===========================================================================
bool FuncConsolidateBy::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    info.method = m_method;
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   FuncTimeShift
*
***/

namespace {
class FuncTimeShift : public IFuncBase<FuncTimeShift> {
    IFuncInstance * onFuncBind(vector<FuncArg> && args) override;
    void onFuncAdjustContext(FuncContext * context) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

    Duration m_shift{};
};
} // namespace
static auto s_timeShift = FuncTimeShift::Factory("timeShift", "Transform")
    .arg("query", FuncArgInfo::kQuery, true)
    .arg("timeShift", FuncArgInfo::kString, true);

//===========================================================================
IFuncInstance * FuncTimeShift::onFuncBind(vector<FuncArg> && args) {
    auto tmp = string(args[0].string.get());
    if (tmp[0] != '+' && tmp[0] != '-')
        tmp = "-" + tmp;
    if (!parse(&m_shift, tmp.c_str()))
        return nullptr;
    return this;
}

//===========================================================================
void FuncTimeShift::onFuncAdjustContext(FuncContext * context) {
    context->first += m_shift;
    context->last += m_shift;
}

//===========================================================================
bool FuncTimeShift::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    if (info.samples) {
        info.name = addFuncName(type(), info.name);
        info.samples = SampleList::dup(*info.samples);
        auto & first = info.samples->first;
        first -= m_shift;
        first -= first.time_since_epoch() % info.samples->interval;
    }
    notify->onFuncOutput(info);
    return true;
}


/****************************************************************************
*
*   Private
*
***/

static vector<IFuncFactory *> s_funcVec;
static vector<TokenTable::Token> s_funcTokens;
static TokenTable s_funcTbl;


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
shared_ptr<char[]> Eval::addFuncName(
    Function::Type ftype,
    const shared_ptr<char[]> & prev
) {
    auto fname = string_view(toString(ftype, "UNKNOWN"));
    auto prevLen = strlen(prev.get());
    auto newLen = prevLen + fname.size() + 2;
    auto out = shared_ptr<char[]>(new char[newLen + 1]);
    auto ptr = out.get();
    memcpy(ptr, fname.data(), fname.size());
    ptr += fname.size();
    *ptr++ = '(';
    memcpy(ptr, prev.get(), prevLen);
    if (newLen <= 1000) {
        ptr += prevLen;
        *ptr++ = ')';
        *ptr = 0;
    } else {
        out[996] = out[997] = out[998] = '.';
        out[999] = 0;
    }
    return out;
}

//===========================================================================
const TokenTable & funcEnums() {
    return s_funcTbl;
}

//===========================================================================
const List<IFuncFactory> & funcFactories() {
    return s_factories;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void funcInitialize() {
    if (s_funcTbl)
        return;

    funcCombineInitialize();
    funcFilterInitialize();
    funcXfrmListInitialize();
    funcXfrmValueInitialize();

    s_funcVec.push_back(nullptr);
    for (auto && f : s_factories)
        s_funcVec.push_back(&f);
    sort(s_funcVec.begin() + 1, s_funcVec.end(), [](auto & a, auto & b) {
        return a->m_names.front() < b->m_names.front();
    });
    for (unsigned i = 1; i < s_funcVec.size(); ++i) {
        auto & f = *s_funcVec[i];
        f.m_type = (Function::Type) i;
        for (auto && n : f.m_names) {
            auto & token = s_funcTokens.emplace_back();
            token.id = f.m_type;
            token.name = n.c_str();
        }
    }
    s_funcTbl = TokenTable{s_funcTokens.data(), s_funcTokens.size()};
}

//===========================================================================
unique_ptr<IFuncInstance> funcCreate(Function::Type type) {
    assert(type < s_funcVec.size());
    return s_funcVec[type]->onFactoryCreate();
}

//===========================================================================
const char * toString(Eval::Function::Type ftype, const char def[]) {
    return tokenTableGetName(s_funcTbl, ftype, def);
}

//===========================================================================
Function::Type fromString(string_view src, Function::Type def) {
    return tokenTableGetEnum(s_funcTbl, src, def);
}

