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
RadixDigits::RadixDigits(
    size_t pageSize, 
    size_t rootOffset,
    size_t pageOffset,
    size_t maxPage
) {
    init(pageSize, rootOffset, pageOffset, maxPage);
}

//===========================================================================
void RadixDigits::init(
    size_t pageSize, 
    size_t rootOffset,
    size_t pageOffset,
    size_t maxPage
) {
    assert(rootOffset < pageSize + sizeof(uint32_t));
    assert(pageOffset < pageSize + sizeof(uint32_t));
    assert(maxPage <= numeric_limits<uint32_t>::max());
    m_pageSize = pageSize;
    m_rootOffset = rootOffset;
    m_pageOffset = pageOffset;
    m_maxPage = maxPage;
    auto rents = rootEntries();
    auto pents = pageEntries();
    size_t m = 1;
    while (m * rents < maxPage) {
        m *= pents;
        m_divs.insert(m_divs.begin(), (uint32_t) m);
    }
}

//===========================================================================
size_t RadixDigits::convert(
    int * out, 
    size_t maxDigits, 
    size_t value
) const {
    assert(maxDigits > m_divs.size());

    int * base = out;
    auto rents = rootEntries();
    auto pents = pageEntries();

    for (;;) {
        *out++ = (int) (value % pents);
        if (value < rents)
            break;
        value /= pents;
    }
    reverse(base, out);
    return out - base;

    //int * base = out;
    //size_t i = 0;
    //for (; i < m_divs.size(); ++i) {
    //    if (value >= m_divs[i]) {
    //        for (;;) {
    //            auto v = value / m_divs[i];
    //            *out++ = (int) v;
    //            value %= m_divs[i];
    //            if (++i == m_divs.size())
    //                break;
    //        }
    //        break;
    //    }
    //}
    //*out++ = (int) value;
    //return out - base;
}

//===========================================================================
size_t RadixDigits::maxDigits() const {
    return m_divs.size();
}

//===========================================================================
size_t RadixDigits::rootEntries() const {
    return (uint32_t) (m_pageSize - m_rootOffset) / sizeof(uint32_t);
}

//===========================================================================
size_t RadixDigits::pageEntries() const {
    return (uint32_t) (m_pageSize - m_pageOffset) / sizeof(uint32_t);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
ostream & operator<< (ostream & os, const RadixDigits & rd) {
    os << rd.m_pageSize << ' ' << rd.m_rootOffset << ' ' 
        << rd.m_pageOffset << ' ';
    for (int i = 0; i < rd.m_divs.size(); ++i) {
        if (i) os << ':';
        os << rd.m_divs[i];
    }
    os << ' ' << rd.m_maxPage;
    return os;
}
