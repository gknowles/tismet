// tokentable.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   TokenTable
*
***/

//===========================================================================
static size_t mathPow2Ceil (size_t num) {
#if 0
    unsigned long k;
    _BitScanReverse64(&k, num);
    return (size_t) 1 << (k + 1);
#endif
#if 0
    size_t k = 0x8000'0000;
    k = (k > num) ? k >> 16 : k << 16;
    k = (k > num) ? k >> 8 : k << 8;
    k = (k > num) ? k >> 4 : k << 4;
    k = (k > num) ? k >> 2 : k << 2;
    k = (k > num) ? k >> 1 : k << 1;
    return k;
#endif
#if 1
    num -= 1;
    num |= (num >> 1);
    num |= (num >> 2);
    num |= (num >> 4);
    num |= (num >> 8);
    num |= (num >> 16);
    num |= (num >> 32);
    num += 1;
    return num;
#endif
#if 0
#pragma warning(disable : 4706) // assignment within conditional expression
    size_t j, k;
    (k = num & 0xFFFF'FFFF'0000'0000) || (k = num);
    (j = k & 0xFFFF'0000'FFFF'0000) || (j = k);
    (k = j & 0xFF00'FF00'FF00'FF00) || (k = j);
    (j = k & 0xF0F0'F0F0'F0F0'F0F0) || (j = k);
    (k = j & 0xCCCC'CCCC'CCCC'CCCC) || (k = j);
    (j = k & 0xAAAA'AAAA'AAAA'AAAA) || (j = k);
    return j << 1;
#endif
}

//===========================================================================
TokenTable::TokenTable (const Token * src, size_t count) {
    size_t num = mathPow2Ceil(2 * count);
    m_names.resize(num);
    if (num < 2) {
        if (num == 1) {
            Value & val = m_names.front();
            m_hashLen = (int) strlen(src->name);
            val.hash = strHash(src->name);
            val.id = src->id;
            val.name = src->name;
        }
        return;
    }

    m_hashLen = 0;
    const Token * eptr = src + count;
    for (const Token * a = src + 1; a != eptr; ++a) {
        for (const Token * b = src; b != a; ++b) {
            int pos = 0;
            while (a->name[pos] == b->name[pos]) {
                if (!a->name[pos]) 
                    goto next_a;
                pos += 1;
            }
            if (pos >= m_hashLen)
                m_hashLen = pos + 1;
        }
    next_a:
        ;
    }

    Value * base = m_names.data();
    for (const Token * a = src; a != eptr; ++a) {
        size_t hash = strHash(a->name, m_hashLen);
        size_t pos = hash;
        for (;;) {
            pos %= num;
            Value & val = base[pos];
            if (!val.name) {
                val.hash = hash;
                val.id = a->id;
                val.name = a->name;
                break;
            }
            if (val.hash == hash && !strcmp(val.name, a->name))
                break;
            pos += 1;
        }
    }
}

//===========================================================================
bool TokenTable::find (int * out, const char name[]) const {
    size_t hash = strHash(name, m_hashLen);
    size_t pos = hash;
    size_t num = size(m_names);
    for (;;) {
        pos %= num;
        const Value & val = m_names[pos];
        if (!val.name)
            break;
        if (val.hash == hash && !strcmp(val.name, name)) {
            *out = val.id;
            return true;
        }
        pos += 1;
    }
    *out = numeric_limits<remove_reference<decltype(*out)>::type>::max();
    return false;
}

//===========================================================================
bool TokenTable::find (const char * const * out, int id) const {
    return false;
}

//===========================================================================
TokenTable::Iterator TokenTable::begin () const {
    return Iterator{nullptr};
}

//===========================================================================
TokenTable::Iterator TokenTable::end () const {
    return Iterator{nullptr};
}


} // namespace
