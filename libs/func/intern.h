// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet func

#include "cppconf/cppconf.h"

#include "core/list.h"
#include "core/tokentable.h"

#include <string>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   Declarations
*
***/

namespace Eval {

struct FuncArgInfo {
    enum Type {
        kAggFunc,
        kNum,
        kNumOrString,
        kQuery,
        kString,
    };
    std::string name;
    Type type;
    bool require{false};
    bool multiple{false};
};

class IFuncFactory
    : public Dim::ListBaseLink<>
    , public Dim::IFactory<IFuncInstance> {
public:
    IFuncFactory(std::string_view name, std::string_view group);
    IFuncFactory(const IFuncFactory & from);
    IFuncFactory(IFuncFactory && from);

    virtual IFuncFactory & arg(
        std::string_view name,
        FuncArgInfo::Type type,
        bool require = false,
        bool multiple = false
    );

    // Inherited via IFactory
    std::unique_ptr<IFuncInstance> onFactoryCreate() override = 0;

    Function::Type m_type{};
    std::vector<std::string> m_names;
    std::string m_group;
    std::vector<FuncArgInfo> m_args;
};

} // namespace

const Dim::TokenTable & funcEnums();
const Dim::TokenTable & funcAggEnums();
const Dim::List<Eval::IFuncFactory> & funcFactories();
