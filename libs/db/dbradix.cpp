// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbradix.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   DbRadix
*
***/

//===========================================================================
DbRadix::DbRadix(
    size_t pageSize,
    size_t rootOffset,
    size_t pageOffset,
    size_t maxPage
) {
    init(pageSize, rootOffset, pageOffset, maxPage);
}

//===========================================================================
void DbRadix::init(
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
}

//===========================================================================
size_t DbRadix::convert(
    int * out,
    size_t maxDigits,
    size_t value
) const {
    int * base = out;
    auto rents = rootEntries();
    auto pents = pageEntries();

    for (size_t i = 0;; ++i) {
        assert(i < maxDigits);
        *out++ = (int) (value % pents);
        if (value < rents)
            break;
        value /= pents;
    }
    reverse(base, out);
    return out - base;
}

//===========================================================================
size_t DbRadix::rootEntries() const {
    return (uint32_t) (m_pageSize - m_rootOffset) / sizeof(uint32_t);
}

//===========================================================================
size_t DbRadix::pageEntries() const {
    return (uint32_t) (m_pageSize - m_pageOffset) / sizeof(uint32_t);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
ostream & operator<< (ostream & os, const DbRadix & rd) {
    os << rd.m_pageSize << ' ' << rd.m_rootOffset << ' '
        << rd.m_pageOffset << ' ' << rd.m_maxPage;
    return os;
}
