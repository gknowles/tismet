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
inline bool QueryParser::onArgNumEnd () {
    double value;
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
        value = m_int * pow(10.0, m_exp);
        m_exp = 0;
        m_frac = 0;
    } else {
        value = (double) m_int;
    }
    m_int = 0;

    addNumArg(m_query, m_nodes.back(), value);
    return true;
}

//===========================================================================
inline bool QueryParser::onArgQueryStart () {
    return true;
}

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
    assert(m_nodes.back()->type > QueryInfo::kBeforeFirstFunc);
    assert(m_nodes.back()->type < QueryInfo::kAfterLastFunc);
    m_nodes.pop_back();
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
inline bool QueryParser::onPathStart (const char * ptr) {
    auto path = m_nodes.empty()
        ? addPath(m_query)
        : addPathArg(m_query, m_nodes.back());
    m_nodes.push_back(path);
    return true;
}

//===========================================================================
inline bool QueryParser::onPathEnd (const char * eptr) {
    assert(m_nodes.back()->type == QueryInfo::kPath);
    m_nodes.pop_back();
    return true;
}

//===========================================================================
inline bool QueryParser::onPathSegStart (const char * ptr) {
    auto seg = addSeg(m_query, m_nodes.back());
    m_nodes.push_back(seg);
    return true;
}

//===========================================================================
inline bool QueryParser::onPathSegEnd (const char * eptr) {
    assert(m_nodes.back()->type == QueryInfo::kPathSeg);
    m_nodes.pop_back();
    return true;
}

//===========================================================================
inline bool QueryParser::onSclRangeEndChar (char last) {
    for (unsigned ch = m_charStart + 1; ch <= (unsigned) last; ++ch)
        m_chars.set(ch);
    return true;
}

//===========================================================================
inline bool QueryParser::onSclSingleChar (char ch) {
    m_charStart = ch;
    m_chars.set((unsigned) ch);
    return true;
}

//===========================================================================
inline bool QueryParser::onSegBlotEnd () {
    addSegBlot(m_query, m_nodes.back());
    return true;
}

//===========================================================================
inline bool QueryParser::onSegCharListEnd () {
    addSegChoices(m_query, m_nodes.back(), m_chars);
    return true;
}

//===========================================================================
inline bool QueryParser::onSegLiteralStart (const char * ptr) {
    m_start = ptr;
    return true;
}

//===========================================================================
inline bool QueryParser::onSegLiteralEnd (const char * eptr) {
    addSegLiteral(
        m_query,
        m_nodes.back(),
        std::string_view(m_start, eptr - m_start)
    );
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrListStart () {
    auto sl = addSegStrChoices(m_query, m_nodes.back());
    m_nodes.push_back(sl);
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrListEnd () {
    assert(m_nodes.back()->type == QueryInfo::kSegStrChoice);
    m_nodes.pop_back();
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrValStart (const char * ptr) {
    m_start = ptr;
    return true;
}

//===========================================================================
inline bool QueryParser::onSegStrValEnd (const char * eptr) {
    addSegChoice(
        m_query,
        m_nodes.back(),
        std::string_view(m_start, eptr - m_start)
    );
    return true;
}


/****************************************************************************
*
*   Query functions
*
***/

//===========================================================================
inline bool QueryParser::onFnMaximumAboveStart () {
    return startFunc(QueryInfo::kFnMaximumAbove);
}

//===========================================================================
inline bool QueryParser::onFnSumStart () {
    return startFunc(QueryInfo::kFnSum);
}
