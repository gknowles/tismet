// radix.cpp - tismet tsdata
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
RadixDigits::RadixDigits(size_t blkSize, size_t maxPage) 
    : m_blkSize{blkSize}
    , m_maxPage{maxPage}
{
    assert(maxPage <= numeric_limits<uint32_t>::max());
    auto ents = (uint32_t) (blkSize / sizeof(uint32_t));
    size_t m = ents;
    while (m < m_maxPage) {
        m_divs.insert(m_divs.begin(), (uint32_t) m);
        m *= ents;
    }
}

//===========================================================================
void RadixDigits::convert(int * digits, size_t maxDigits, uint32_t value) {
    assert(maxDigits > m_divs.size());
    size_t i = 0;
    for (; i < m_divs.size(); ++i) {
        if (value >= m_divs[i]) {
            for (;;) {
                auto v = value / m_divs[i];
                *digits++ = v;
                value %= m_divs[i];
                if (++i == m_divs.size())
                    break;
            }
            break;
        }
    }
    *digits++ = value;
    *digits = -1;
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

