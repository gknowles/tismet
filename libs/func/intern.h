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

template<typename T>
class FuncFactory : public IFuncFactory {
public:
    using IFuncFactory::IFuncFactory;
    std::unique_ptr<IFuncInstance> onFactoryCreate() override;

    FuncFactory & arg(
        std::string_view name,
        FuncArg::Type type,
        bool require = false,
        bool multiple = false
    );
    FuncFactory & arg(
        std::string_view name,
        const char enumName[],
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
    bool onFuncApply(IFuncNotify * notify, ResultInfo & info) override;

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
    FuncArg::Type type,
    bool require,
    bool multiple
) {
    auto arg = FuncArgInfo{std::string(name), type, {}, require, multiple};
    m_args.push_back(std::move(arg));
    return *this;
}

//===========================================================================
template<typename T>
Eval::FuncFactory<T> & Eval::FuncFactory<T>::arg(
    std::string_view name,
    const char enumName[],
    bool require,
    bool multiple
) {
    auto arg = FuncArgInfo{
        std::string(name),
        FuncArg::kEnum,
        enumName,
        require,
        multiple
    };
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

//===========================================================================
template<typename T>
bool Eval::IFuncBase<T>::onFuncApply(IFuncNotify * notify, ResultInfo & info) {
    assert(!"onFuncApply not implemented by base function");
    return false;
}
