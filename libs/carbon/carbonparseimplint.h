// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// carbonparseimplint.h - tismet carbon
#pragma once

#include "carbonparseint.h"

#include <string_view>


/****************************************************************************
*
*   CarbonParser
*
*   Implementation of events
*
***/

//===========================================================================
inline bool CarbonParser::onExpMinusEnd () {
    m_expMinus = true;
    return true;
}

//===========================================================================
inline bool CarbonParser::onExpNumChar (char ch) {
    m_exp = 10 * m_exp + (ch - '0');
    return true;
}

//===========================================================================
inline bool CarbonParser::onFracNumChar (char ch) {
    m_int = 10 * m_int + (ch - '0');
    m_frac += 1;
    return true;
}

//===========================================================================
inline bool CarbonParser::onIntNumChar (char ch) {
    m_int = 10 * m_int + (ch - '0');
    return true;
}

//===========================================================================
inline bool CarbonParser::onMetricEnd () {
    m_upd->name = std::string_view(m_nameStart, m_nameEnd - m_nameStart);

    if (m_seconds == -1) {
        m_upd->time = {};
    } else {
        m_upd->time = Dim::timeFromUnix(m_seconds);
    }
    m_seconds = 0;

    if (m_minus) {
        m_minus = false;
        m_int = -m_int;
    }
    if (m_exp || m_frac) {
        if (m_expMinus) {
            m_expMinus = false;
            m_exp = -m_exp;
        }
        m_upd->value = m_int * pow(10.0f, m_exp - m_frac);
        m_exp = 0;
        m_frac = 0;
    } else {
        m_upd->value = (float) m_int;
    }
    m_int = 0;

    return false;
}

//===========================================================================
inline bool CarbonParser::onIntMinusEnd () {
    m_minus = true;
    return true;
}

//===========================================================================
inline bool CarbonParser::onPathStart (char const * ptr) {
    m_nameStart = ptr;
    return true;
}

//===========================================================================
inline bool CarbonParser::onPathEnd (char const * eptr) {
    m_nameEnd = eptr;
    return true;
}

//===========================================================================
inline bool CarbonParser::onNowEnd () {
    m_seconds = -1;
    return true;
}

//===========================================================================
inline bool CarbonParser::onTimepointChar (char ch) {
    m_seconds = 10 * m_seconds + (ch - '0');
    return true;
}
