// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// evalfuncs.cpp - tismet eval
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;
using namespace Eval;
using namespace Query;


/****************************************************************************
*
*   Private
*
***/

namespace {

template<typename T>
class Register {
public:
    Register(Function::Type ftype) {
        registerFunc(ftype, getFactory<FuncNode, T>());
    }
};

template<Function::Type FT, typename T>
class FuncImpl : public FuncNode {
public:
    inline static Register<T> s_register{FT};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static shared_ptr<char[]> addFuncName(
    Function::Type ftype,
    const shared_ptr<char[]> & prev
) {
    auto fname = string_view(Query::getFuncName(ftype, "UNKNOWN"));
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


/****************************************************************************
*
*   FuncAlias
*
***/

namespace {
class FuncAlias : public FuncImpl<Function::kAlias, FuncAlias> {
    void onResult(int resultId, const ResultInfo & info) override;
};
} // namespace

//===========================================================================
void FuncAlias::onResult(int resultId, const ResultInfo & info) {
    ResultInfo out{info};
    if (out.samples)
        out.name = m_args[0].string;
    forwardResult(out, true);
}


/****************************************************************************
*
*   FuncMaximumAbove
*
***/

namespace {
class FuncMaximumAbove
    : public FuncImpl<Function::kMaximumAbove, FuncMaximumAbove>
{
    void onResult(int resultId, const ResultInfo & info) override;
};
} // namespace

//===========================================================================
void FuncMaximumAbove::onResult(int resultId, const ResultInfo & info) {
    ResultInfo out{info};
    if (out.samples) {
        auto limit = m_args[0].number;
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        if (find_if(ptr, eptr, [&](auto val){ return val > limit; }) == eptr) {
            out.name = {};
            out.samples = {};
        }
    }
    forwardResult(out, true);
}


/****************************************************************************
*
*   FuncKeepLastValue
*
***/

namespace {
class FuncKeepLastValue
    : public FuncImpl<Function::kKeepLastValue, FuncKeepLastValue>
{
    Apply onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncKeepLastValue::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto limit = m_args.empty() ? 0 : (int) m_args[0].number;
    auto base = info.samples->samples;
    auto eptr = base + info.samples->count;
    int nans = 0;
    for (; base != eptr; ++base) {
        if (!isnan(*base))
            break;
    }
    for (auto ptr = base; ptr != eptr; ++ptr) {
        if (isnan(*ptr)) {
            if (!nans++)
                base = ptr - 1;
        } else if (nans) {
            if (!limit || nans <= limit) {
                auto val = *base++;
                for (; base != ptr; ++base)
                    *base = val;
            }
            nans = 0;
        }
    }
    if (nans && (!limit || nans <= limit)) {
        auto val = *base++;
        for (; base != eptr; ++base)
            *base = val;
    }
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncDerivative
*
***/

namespace {
class FuncDerivative : public FuncImpl<Function::kDerivative, FuncDerivative> {
    Apply onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncDerivative::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto out = SampleList::alloc(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count - 1;
    *optr++ = NAN;
    for (; ptr != eptr; ++ptr) {
        *optr++ = ptr[1] - ptr[0];
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncNonNegativeDerivative
*
***/

namespace {
class FuncNonNegativeDerivative
    : public FuncImpl<
        Function::kNonNegativeDerivative,
        FuncNonNegativeDerivative>
{
    Apply onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncNonNegativeDerivative::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto limit = m_args.empty() ? HUGE_VAL : m_args[0].number;

    auto out = SampleList::alloc(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    *optr++ = NAN;
    auto prev = (double) NAN;
    for (ptr += 1; ptr != eptr; ++ptr) {
        if (*ptr > limit) {
            prev = *optr++ = NAN;
        } else if (*ptr >= prev) {
            auto next = *ptr;
            *optr++ = *ptr - prev;
            prev = next;
        } else {
            auto next = *ptr;
            *optr++ = *ptr + (limit - prev + 1);
            prev = next;
        }
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncScale
*
***/

namespace {
class FuncScale : public FuncImpl<Function::kScale, FuncScale> {
    Apply onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncScale::onFuncApply(ResultInfo & info) {
    info.name = addFuncName(m_type, info.name);

    auto factor = m_args[0].number;

    auto out = SampleList::alloc(*info.samples);
    auto optr = out->samples;
    auto ptr = info.samples->samples;
    auto eptr = ptr + info.samples->count;
    for (; ptr != eptr; ++ptr) {
        *optr++ = *ptr * factor;
    }
    assert(optr == out->samples + out->count);
    info.samples = out;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncTimeShift
*
***/

namespace {
class FuncTimeShift : public FuncImpl<Function::kTimeShift, FuncTimeShift> {
    Apply onFuncApply(ResultInfo & info) override;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncTimeShift::onFuncApply(ResultInfo & info) {
    auto tmp = string(m_args[0].string.get());
    if (tmp[0] != '+' && tmp[0] != '-')
        tmp = "-" + tmp;
    Duration shift;
    if (!parse(&shift, tmp.c_str()))
        return Apply::kFinished;

    info.name = addFuncName(m_type, info.name);
    info.samples = SampleList::dup(*info.samples);
    info.samples->first += shift;
    return Apply::kForward;
}


/****************************************************************************
*
*   FuncHighestCurrent
*
***/

namespace {
class FuncHighestCurrent
    : public FuncImpl<Function::kHighestCurrent, FuncHighestCurrent>
{
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncHighestCurrent::onResultTask(ResultInfo & info) {
    auto allowed = m_args[0].number;

    // last non-NAN sample in list
    auto best = (double) NAN;
    if (info.samples) {
        for (int i = info.samples->count; i-- > 0;) {
            best = info.samples->samples[i];
            if (!isnan(best))
                break;
        }
    }

    if (!isnan(best)) {
        if (m_best.size() < allowed) {
            m_best.emplace(best, info);
        } else if (auto i = m_best.begin();  i->first < best) {
            m_best.erase(i);
            m_best.emplace(best, info);
        }
    }
    if (!info.more) {
        for (auto && out : m_best) {
            out.second.more = true;
            forwardResult(out.second);
        }
        ResultInfo out{};
        out.target = info.target;
        forwardResult(out);
        m_best.clear();
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   FuncHighestMax
*
***/

namespace {
class FuncHighestMax : public FuncImpl<Function::kHighestMax, FuncHighestMax> {
    Apply onResultTask(ResultInfo & info) override;
    multimap<double, ResultInfo> m_best;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncHighestMax::onResultTask(ResultInfo & info) {
    auto allowed = m_args[0].number;

    // largest non-NAN sample in list
    auto best = -numeric_limits<double>::infinity();
    bool found = false;
    if (info.samples) {
        auto ptr = info.samples->samples;
        auto eptr = ptr + info.samples->count;
        for (; ptr != eptr; ++ptr) {
            if (*ptr > best) {
                best = *ptr;
                found = true;
            }
        }
    }

    if (found) {
        if (m_best.size() < allowed) {
            m_best.emplace(best, info);
        } else if (auto i = m_best.begin();  best > i->first) {
            m_best.erase(i);
            m_best.emplace(best, info);
        }
    }
    if (!info.more) {
        for (auto i = m_best.rbegin(), ei = m_best.rend(); i != ei; ++i) {
            i->second.more = true;
            forwardResult(i->second);
        }
        ResultInfo out{};
        out.target = info.target;
        forwardResult(out);
        m_best.clear();
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   FuncSum
*
***/

namespace {
class FuncSum : public FuncImpl<Function::kSum, FuncSum> {
    Apply onResultTask(ResultInfo & info) override;
    shared_ptr<SampleList> m_samples;
};
} // namespace

//===========================================================================
FuncNode::Apply FuncSum::onResultTask(ResultInfo & info) {
    if (info.samples) {
        if (!m_samples) {
            m_samples = SampleList::dup(*info.samples);
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
                auto spos = 0;
                for (; first < slast; first += interval, ++pos, ++spos)
                    tmp->samples[pos] = m_samples->samples[spos];
                for (; first < last; first += interval, ++pos)
                    tmp->samples[pos] = NAN;
                assert(pos == (int) tmp->count);
                m_samples = tmp;
                slast = first;
                first = m_samples->first;
            }
            auto ipos = 0;
            auto pos = (info.samples->first - first) / interval;
            first = info.samples->first;
            for (; first < ilast; first += interval, ++pos, ++ipos) {
                auto & sval = m_samples->samples[pos];
                auto & ival = info.samples->samples[ipos];
                if (isnan(sval)) {
                    sval = ival;
                } else if (!isnan(ival)) {
                    sval += ival;
                }
            }
            assert(pos <= m_samples->count);
        } else {
            // TODO: normalize and consolidate incompatible lists
            logMsgError() << "summing incompatible series";
        }
    }

    if (!info.more) {
        ResultInfo out;
        out.target = info.target;
        out.name = addFuncName(m_type, info.target);
        out.samples = move(m_samples);
        out.more = false;
        forwardResult(out);
    }
    return Apply::kSkip;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
// This function exists to trigger the static initializers in this file that
// then register the function node factories.
void Eval::initializeFuncs()
{}
