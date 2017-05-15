// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// radix.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/


/****************************************************************************
*
*   Variables
*
***/


/****************************************************************************
*
*   RadixDigits
*
***/

//===========================================================================
RadixDigits::RadixDigits(size_t blkSize, size_t maxPage) {
    init(blkSize, maxPage);
}

//===========================================================================
void RadixDigits::init(size_t blkSize, size_t maxPage) {
    m_blkSize = blkSize;
    m_maxPage = maxPage;
    assert(maxPage <= numeric_limits<uint32_t>::max());
    auto ents = (uint32_t) pageEntries();
    size_t m = ents;
    while (m < m_maxPage) {
        m_divs.insert(m_divs.begin(), (uint32_t) m);
        m *= ents;
    }
}

//===========================================================================
size_t RadixDigits::convert(
    int * out, 
    size_t maxDigits, 
    uint32_t value
) const {
    assert(maxDigits > m_divs.size());
    int * base = out;
    size_t i = 0;
    for (; i < m_divs.size(); ++i) {
        if (value >= m_divs[i]) {
            for (;;) {
                auto v = value / m_divs[i];
                *out++ = v;
                value %= m_divs[i];
                if (++i == m_divs.size())
                    break;
            }
            break;
        }
    }
    *out++ = value;
    return out - base;
}

//===========================================================================
size_t RadixDigits::pageEntries() const {
    return m_blkSize / sizeof(uint32_t);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
ostream & operator<< (ostream & os, const RadixDigits & rd) {
    os << rd.m_blkSize << ' ' << rd.m_offset << ' ';
    for (int i = 0; i < rd.m_divs.size(); ++i) {
        if (i) os << ':';
        os << rd.m_divs[i];
    }
    os << ' ' << rd.m_maxPage;
    return os;
}
