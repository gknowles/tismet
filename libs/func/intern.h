// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet func


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

    // Inherited via IFactory
    std::unique_ptr<IFuncInstance> onFactoryCreate() override = 0;

    Function::Type m_type{};
    std::vector<std::string> m_names;
    std::string m_group;
    std::vector<FuncArgInfo> m_args;
};

template<typename T>
class FuncFactory : public IFuncFactory {
public:
    using IFuncFactory::IFuncFactory;
    std::unique_ptr<IFuncInstance> onFactoryCreate() override;

    FuncFactory & arg(
        std::string_view name,
        FuncArgInfo::Type type,
        bool require = false,
        bool multiple = false
    );
    FuncFactory & alias(std::string_view name);
};

template<typename T>
class IFuncBase : public IFuncInstance {
public:
    using Factory = FuncFactory<T>;

public:
    Function::Type type() const override;

    IFuncInstance * onFuncBind(std::vector<FuncArg> && args) override;
    void onFuncAdjustContext(FuncContext * context) override;
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override = 0;

protected:
    Dim::Duration m_pretime{};
    unsigned m_presamples{0};

private:
    friend Factory;
    Function::Type m_type{};
};

std::shared_ptr<char[]> addFuncName(
    Function::Type ftype,
    const std::shared_ptr<char[]> & prev
);

} // namespace

const Dim::TokenTable & funcEnums();
const Dim::TokenTable & funcAggEnums();
const Dim::List<Eval::IFuncFactory> & funcFactories();

void funcCombineInitialize();
void funcFilterInitialize();
void funcXfrmListInitialize();
void funcXfrmValueInitialize();


/****************************************************************************
*
*   FuncFactory
*
***/

//===========================================================================
template<typename T>
std::unique_ptr<Eval::IFuncInstance> Eval::FuncFactory<T>::onFactoryCreate() {
    auto ptr = std::make_unique<T>();
    ptr->m_type = m_type;
    return move(ptr);
}

//===========================================================================
template<typename T>
Eval::FuncFactory<T> & Eval::FuncFactory<T>::arg(
    std::string_view name,
    FuncArgInfo::Type type,
    bool require,
    bool multiple
) {
    auto arg = FuncArgInfo{std::string(name), type, require, multiple};
    m_args.push_back(std::move(arg));
    return *this;
}

//===========================================================================
template<typename T>
Eval::FuncFactory<T> & Eval::FuncFactory<T>::alias(std::string_view name) {
    m_names.push_back(std::string(name));
    return *this;
}


/****************************************************************************
*
*   IFuncBase
*
***/

//===========================================================================
template<typename T>
Eval::Function::Type Eval::IFuncBase<T>::type() const {
    return m_type;
}

//===========================================================================
template<typename T>
Eval::IFuncInstance * Eval::IFuncBase<T>::onFuncBind(
    std::vector<FuncArg> && args
) {
    return this;
}

//===========================================================================
template<typename T>
void Eval::IFuncBase<T>::onFuncAdjustContext(Eval::FuncContext * context) {
    context->pretime += m_pretime;
    context->presamples += m_presamples;
}
