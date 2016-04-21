// util.h - dim core
#pragma once

#include "dim/config.h"

namespace Dim {

#include <climits>
#include <type_traits>


/****************************************************************************
*
*   Hashing
*
***/

size_t strHash (const char src[]);

// calculates hash up to trailing null or maxlen, whichever comes first
size_t strHash (const char src[], size_t maxlen);


/****************************************************************************
*
*   String conversions
*
***/

template <typename T>
constexpr int maxIntegralChars () {
    return numeric_limits<T>::is_signed
        ? 1 + ((CHAR_BIT * sizeof(T) - 1) * 301L + 999L) / 1000L
        : (CHAR_BIT * sizeof(T) * 301L + 999L) / 1000L;
}

template <typename T>
class IntegralStr {
public:
    IntegralStr (T val);
    const char * set (T val);
    operator const char * () const;

private:
    using Signed = typename std::make_signed<T>::type;
    using Unsigned = typename std::make_unsigned<T>::type;

    const char * internalSet (Signed val);
    const char * internalSet (Unsigned val);

    char data[maxIntegralChars<T>() + 1];
};

//===========================================================================
template <typename T>
IntegralStr<T>::IntegralStr (T val) { 
    internalSet(val); 
}

//===========================================================================
template <typename T>
const char * IntegralStr<T>::set (T val) {
    return internalSet(val);
}

//===========================================================================
template <typename T>
IntegralStr<T>::operator const char * () const { 
    return data; 
}

//===========================================================================
template <typename T>
const char * IntegralStr<T>::internalSet (Unsigned val) {
    if (!val) {
        data[0] = '0';
        data[1] = 0;
    } else {
        char * ptr = data;
        unsigned i = 0;
        for (;;) {
            ptr[i] = (val % 10) + '0';
            val /= 10;
            i += 1;
            if (!val)
                break;
        }
        ptr[i] = 0;
        for (; i > 1; i -= 2) {
            swap(*ptr, ptr[i]);
            ptr += 1;
        }
    }
    return data;
}

//===========================================================================
template <typename T>
const char * IntegralStr<T>::internalSet (Signed val) {
    if (!val) {
        data[0] = '0';
        data[1] = 0;
    } else {
        char * ptr = data;
        if (val < 0) {
            *ptr++ = '-';
            val = -val;
        }
        unsigned i = 0;
        for (;;) {
            ptr[i] = (val % 10) + '0';
            val /= 10;
            i += 1;
            if (!val)
                break;
        }
        ptr[i] = 0;
        for (; i > 1; i -= 2) {
            swap(*ptr, ptr[i]);
            ptr += 1;
        }
    }
    return data;
}


/****************************************************************************
*
*   Containers
*
***/

template <typename T>
class ForwardListIterator {
protected:
    T * m_current{nullptr};
public:
    ForwardListIterator (T * node) : m_current(node) {}
    bool operator!= (const ForwardListIterator & right) {
        return m_current != right.m_current;
    }
    auto operator++ () {
        m_current = m_current->m_next;
        return *this;
    }
    T & operator* () {
        assert(m_current);
        return *m_current;
    }
};


} // namespace
