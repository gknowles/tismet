// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// queryparseimplint.h - tismet query
#pragma once

#include "queryparseint.h"

#include <string_view>


/****************************************************************************
*
*   QueryParser
*
*   Implementation of events
*
***/

//===========================================================================
inline bool QueryParser::onExpMinusEnd () {
    m_expMinus = true;
    return true; 
}

//===========================================================================
inline bool QueryParser::onExpNumChar (char ch) {
    m_exp = 10 * m_exp + (ch - '0');
    return true; 
}

//===========================================================================
inline bool QueryParser::onFracNumChar (char ch) {
    m_int = 10 * m_int + (ch - '0');
    m_frac += 1;
    return true; 
}

//===========================================================================
inline bool QueryParser::onFuncEnd () {
    return true;
}

//===========================================================================
inline bool QueryParser::onIntChar (char ch) {
    m_int = 10 * m_int + (ch - '0');
    return true; 
}

//===========================================================================
inline bool QueryParser::onMinusEnd () {
    m_minus = true;
    return true; 
}

//===========================================================================
inline bool QueryParser::onParamNumEnd () {
    if (m_minus) {
        m_minus = false;
        m_int = -m_int;
    }
    if (m_exp || m_frac) {
        if (m_expMinus) {
            m_expMinus = false;
            m_exp = -m_exp;
        }
        m_exp -= m_frac;
        //m_upd->value = m_int * pow(10.0f, m_exp);
        m_exp = 0;
        m_frac = 0;
    } else {
        //m_upd->value = (float) m_int;
    }
    m_int = 0;
    return true;
}

//===========================================================================
inline bool QueryParser::onParamQueryStart () {
    return true;
}

//===========================================================================
inline bool QueryParser::onPathStart (const char * ptr) {
    m_nameStart = ptr;
    return true; 
}

//    m_upd->name = std::string_view(m_nameStart, m_nameEnd - m_nameStart);

//===========================================================================
inline bool QueryParser::onPathEnd (const char * eptr) {
    m_nameEnd = eptr - 1;
    return true; 
}

//===========================================================================
inline bool QueryParser::onSclEndChar (char ch) {
    return true;
}

//===========================================================================
inline bool QueryParser::onSclSingleChar (char ch) {
    return true;
}

//===========================================================================
inline bool QueryParser::onSclStartChar (char ch) {
    return true;
}

//===========================================================================
inline bool QueryParser::onSegBlotEnd () {
    return true;
}

//===========================================================================
inline bool QueryParser::onSegCharListEnd () {
    return true;
}

//===========================================================================
inline bool QueryParser::onSegLiteralStart (const char * ptr) {
    m_nameStart = ptr;
    return true; 
}

//===========================================================================
inline bool QueryParser::onSegLiteralEnd (const char * eptr) {
    m_nameEnd = eptr - 1;
    return true; 
}

//===========================================================================
inline bool QueryParser::onSegStrListEnd () {
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrValStart (const char * ptr) {
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrValEnd (const char * eptr) {
    return true;
}

//===========================================================================
inline bool QueryParser::onSumEnd () {
    return true;
}
