// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// qryparseimplint.h - tismet query
#pragma once

#include "qryparseint.h"

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
    assert(m_nodes.back()->type == Query::kFunc);
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
inline bool QueryParser::onPathStart () {
    auto path = m_nodes.empty()
        ? addPath(m_query)
        : addPathArg(m_query, m_nodes.back());
    m_nodes.push_back(path);
    return true;
}

//===========================================================================
inline bool QueryParser::onPathEnd () {
    assert(m_nodes.back()->type == Query::kPath);
    endPath(m_query, m_nodes.back());
    m_nodes.pop_back();
    return true;
}

//===========================================================================
inline bool QueryParser::onPathSegStart () {
    auto seg = addSeg(m_query, m_nodes.back());
    m_nodes.push_back(seg);
    return true;
}

//===========================================================================
inline bool QueryParser::onPathSegEnd () {
    auto i = m_nodes.rbegin();
    assert((*i)->type == Query::kPathSeg);
    endSeg(m_query, *i, *(i + 1));
    m_nodes.pop_back();
    m_pathSeg = true;
    return true;
}

//===========================================================================
inline bool QueryParser::onSclRangeEndChar (char last) {
    for (unsigned i = m_charStart + 1; i <= (unsigned) last; ++i)
        m_chars.set(i);
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
    addSegCharChoices(m_query, m_nodes.back(), m_chars);
    m_chars.reset();
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
inline bool QueryParser::onSslSegsStart () {
    auto sl = addSegSegChoices(m_query, m_nodes.back());
    m_nodes.push_back(sl);
    m_pathSeg = false;
    return true;
}

//===========================================================================
inline bool QueryParser::onSslSegsEnd () {
    assert(m_nodes.back()->type == Query::kSegSegChoice);
    onSslCommaEnd();
    m_nodes.pop_back();
    return true;
}

//===========================================================================
inline bool QueryParser::onSslCommaEnd () {
    if (!m_pathSeg) {
        onPathSegStart();
        addSegEmpty(m_query, m_nodes.back());
        onPathSegEnd();
    }
    m_pathSeg = false;
    return true;
}

//===========================================================================
inline bool QueryParser::onStringStart (const char * ptr) {
    m_start = ptr;
    return true;
}

//===========================================================================
inline bool QueryParser::onStringEnd (const char * eptr) {
    addStringArg(
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

#include "qryparseimplfnint.h"
