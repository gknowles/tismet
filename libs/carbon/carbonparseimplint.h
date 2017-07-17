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
inline bool CarbonParser::onExpMinusEnd (const char * eptr) {
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
inline bool CarbonParser::onIntChar (char ch) {
    m_int = 10 * m_int + (ch - '0');
    return true; 
}

//===========================================================================
inline bool CarbonParser::onMetricEnd (const char * eptr) {
    m_upd->name = std::string_view(m_nameStart, m_nameEnd - m_nameStart);

    m_upd->time = Dim::Clock::from_time_t(m_seconds);
    m_seconds = 0;

    if (m_minus) {
        m_minus = false;
        m_int = -m_int;
    }
    if (m_exp) {
        if (m_expMinus) {
            m_expMinus = false;
            m_exp = -m_exp;
        }
        m_exp -= m_frac;
        m_upd->value = m_int * pow(10.0f, m_exp);
        m_exp = 0;
        m_frac = 0;
    } else {
        m_upd->value = (float) m_int;
    }
    m_int = 0;

    return false; 
}

//===========================================================================
inline bool CarbonParser::onMinusEnd (const char * eptr) {
    m_minus = true;
    return true; 
}

//===========================================================================
inline bool CarbonParser::onPathStart (const char * ptr) {
    m_nameStart = ptr;
    return true; 
}

//===========================================================================
inline bool CarbonParser::onPathEnd (const char * eptr) {
    m_nameEnd = eptr - 1;
    return true; 
}

//===========================================================================
inline bool CarbonParser::onTimestampChar (char ch) {
    m_seconds = 10 * m_seconds + (ch - '0');
    return true; 
}
