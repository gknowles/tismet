// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// intern.h - tismet test


/****************************************************************************
*
*   Declarations
*
***/

class ITest : public Dim::ListBaseLink<> {
public:
    ITest (std::string_view name, std::string_view desc, bool verbose = false);
    virtual ~ITest() = default;

    virtual void onTestRun () = 0;

    std::string_view name() const { return m_name; }

protected:
    bool m_verbose{false};

private:
    std::string m_name;
};
