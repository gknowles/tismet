// cmdline.h - dim services
#pragma once

#include "dim/config.h"

#include <sstream>
#include <string>
#include <vector>

namespace Dim {
namespace CmdLine {


/****************************************************************************
*
*   OptBase
*
***/

class OptBase {
public:
    OptBase (
        const std::string & names, 
        const std::string & desc,
        bool multiple,
        bool boolean
    );
    virtual ~OptBase ();

    bool Explicit () const { return m_explicit; }

protected:
    virtual bool ParseValue (const std::string & value) = 0;

private:
    bool m_explicit{false}; // the value was explicitly set
    bool m_bool{false};     // the value is a bool (no separate value)
    bool m_multiple{false}; // there can be multiple values
};


/****************************************************************************
*
*   Option
*
***/

template <typename T>
class Option : public OptBase {
public:
    Option (
        const std::string & names,
        const std::string & desc,
        const T & def = {}
    );

    operator T& () { return m_value; }

private:
    bool ParseValue (const std::string & value) override;

    T m_value;
};

//===========================================================================
template <typename T>
inline Option<T>::Option (
    const std::string & names,
    const std::string & desc,
    const T & def
) 
    : OptBase{names, desc, false, false}
    , m_value{def}
{}

//===========================================================================
template <>
inline Option<bool>::Option (
    const std::string & names,
    const std::string & desc,
    const bool & def
) 
    : OptBase{names, desc, false, true}
    , m_value{def}
{}

//===========================================================================
template <typename T>
inline bool Option<T>::ParseValue (const std::string & value) {
    std::stringstream interpreter;
    if (!(interpreter << value)
        || !(interpreter >> m_value)
        || !(interpreter >> std::ws).eof()
    ) {
        m_value = {};
        return false;
    }
    return true; 
}


/****************************************************************************
*
*   OptionVector
*
***/

template <typename T>
class OptionVector : public OptBase {
public:
    OptionVector (
        const std::string & names,
        const std::string & desc
    );

    operator std::vector<T>& () { return m_values; }

private:
    bool ParseValue (const std::string & value) override;

    std::vector<T> m_values; // the value
};

//===========================================================================
template <typename T>
inline OptionVector<T>::OptionVector (
    const std::string & names,
    const std::string & desc
) 
    : OptBase{names, desc, true, false}
{}

//===========================================================================
template <>
inline OptionVector<bool>::OptionVector (
    const std::string & names,
    const std::string & desc
) 
    : OptBase{names, desc, true, true}
{}

//===========================================================================
template <typename T>
inline bool OptionVector<T>::ParseValue (const std::string & value) {
    std::stringstream interpreter;
    T tmp;
    if (!(interpreter << value)
        || !(interpreter >> tmp)
        || !(interpreter >> std::ws).eof()
    ) {
        return false;
    }
    m_values.push_back(tmp);
    return true; 
}


/****************************************************************************
*
*   General
*
***/

bool ParseOptions (int argc, char ** argv);
bool ParseOptions (const char cmdline[]);

void PrintError (std::ostream & os);
void PrintHelp (std::ostream & os);

} // namespace
} // namespace
