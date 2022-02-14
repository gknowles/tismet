// Copyright Glen Knowles 2018 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet test


/****************************************************************************
*
*   Declarations
*
***/

class ITest : public Dim::ListLink<> {
public:
    ITest(std::string_view name, std::string_view desc);
    virtual ~ITest() = default;

    virtual void onTestRun() = 0;

    const std::string & name() const { return m_name; }

protected:
    Dim::Cli m_cli;

private:
    std::string m_name;
};
