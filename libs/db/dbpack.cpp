// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbpack.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

constexpr struct {
    int bits;
    int encoded;
    int factor;
} kExponentInfo[8] = {
    { 64, 0x78, 1 },
    { 61, 0x79, 10 },
    { 58, 0x7a, 100 },
    { 55, 0x7b, 1'000 },
    { 51, 0x7c, 10'000 },
    { 48, 0x7d, 100'000 },
    { 45, 0x7e, 1'000'000 },
    { 41, 0x7f, 10'000'000 },
};


/****************************************************************************
*
*   DbPack
*
***/

//===========================================================================
DbPack::DbPack(void * out, size_t outLen, size_t unusedBits)
    : m_base{(unsigned char *) out}
    , m_count{outLen}
    , m_unusedBits{(uint8_t) unusedBits}
{
    assert(unusedBits <= 7);
}

//===========================================================================
DbPack::DbPack(const DbUnpackIter & unpack) {
    m_state = unpack.state();
}

//===========================================================================
void DbPack::retarget(void * out, size_t outLen, size_t unusedBits) {
    assert(unusedBits <= 7);
    m_base = (unsigned char *) out;
    m_count = outLen;
    m_unusedBits = (uint8_t) unusedBits;
}

//===========================================================================
bool DbPack::put(TimePoint time, double value) {
    return put(time) && put(value);
}

//===========================================================================
bool DbPack::put(TimePoint time) {
    assert(time > m_state.sample.time);
    auto dt = time - m_state.sample.time;
    auto ddt = (dt - m_state.dt).count();
    m_state.sample.time = time;
    m_state.dt = dt;
    if (ddt == 0) {
        // Same as previous time.
        // '0'
        return bitput(1, 0);
    }
    if (ddt % kExponentInfo[m_state.expBits].factor) {
        // Too small for previous exponent, record new exponent.
        // '1111' + exponent (3 bits)
        do {
            m_state.expBits -= 1;
        } while (ddt % kExponentInfo[m_state.expBits].factor);
        if (!bitput(7, kExponentInfo[m_state.expBits].encoded))
            return false;
    }
    ddt /= kExponentInfo[m_state.expBits].factor;
    if (ddt < 0) {
        if (ddt >= -64) {
            // ddt within [-64, -1]
            // '10' + ddt (7 bits)
            return bitput(9, (0b10 << 7) | ddt & 0x7f);
        } else if (ddt >= -2048) {
            // ddt within [-2048, -65]
            // '110' + ddt (12 bits)
            return bitput(15, (0b110 << 12) | ddt & 0xfff);
        } else {
            auto bits = kExponentInfo[m_state.expBits].bits;
            if (bits < 60) {
                // ddt within [-2^59, -2049]
                // '1110' + ddt (41 - 58 bits, depending on exponent)
                return bitput(
                    4 + bits,
                    (0b1110 << bits) | ddt & ((1 << bits) - 1)
                );
            } else {
                // ddt within [-2^63, -2049]
                // '1110' + ddt (61 - 64 bits, depending on exponent)
                return bitcheck(4 + bits)
                    && bitput(4, 0b1110)
                    && bitput(bits, ddt & ((1 << bits) - 1));
            }
        }
    } else {
        // Adjust downward to make space for 2^N at the expense of zero,
        // which should already be filtered out.
        assert(ddt != 0);
        ddt -= 1;

        if (ddt <= 63) {
            // ddt within [1, 64]
            // '10' + (ddt - 1) (7 bits)
            return bitput(9, (0b10 << 7) | ddt);
        } else if (ddt <= 2047) {
            // ddt within [65, 2048]
            // '110' + (ddt - 1) (12 bits)
            return bitput(15, (0b110 << 12) | ddt);
        } else {
            auto bits = kExponentInfo[m_state.expBits].bits;
            if (bits < 60) {
                // ddt within [2049, 2^59]
                // '1110' + (ddt - 1) (41 - 58 bits, depending on exponent)
                return bitput(4 + bits, (0b1110 << bits) | ddt);
            } else {
                // ddt within [2049, 2^59]
                // '1110' + (ddt - 1) (61 - 64 bits, depending on exponent)
                return bitcheck(4 + bits)
                    && bitput(4, 0b1110)
                    && bitput(bits, ddt);
            }
        }
    }
}

//===========================================================================
bool DbPack::put(double value) {
    auto dv = (uint64_t &) value ^ (uint64_t &) m_state.sample.value;
    m_state.sample.value = value;
    if (!dv) {
        // Same as previous value.
        // '0'
        return bitput(1, 0);
    }

    auto prefix = leadingZeroBits(dv);
    auto len = 64 - prefix - trailingZeroBits(dv);
    if (prefix >= m_state.prefixBits
        && prefix + len <= m_state.prefixBits + m_state.lenBits
    ) {
        // Meaningful bits (i.e. not the leading or trailing zeros) fits
        // within previous range.
        // '10' + meaningful bits
        auto suffix = 64 - m_state.prefixBits - m_state.lenBits;
        return bitcheck(2 + m_state.lenBits)
            && bitput(2, 0b10)
            && bitput(m_state.lenBits, dv >> suffix);
    }

    // Specify new range of meaningful bits as well as the new value.
    // '11' + number of leading zeros (5 bits)
    //      + number of meaningful bits (6 bits)
    //      + meaningful bits
    m_state.prefixBits = (uint8_t) prefix;
    m_state.lenBits = (uint8_t) len;
    auto out = (0b11 << 11) | (m_state.prefixBits << 6) | m_state.lenBits;
    auto suffix = 64 - m_state.prefixBits - m_state.lenBits;
    return bitcheck(13 + m_state.lenBits)
        && bitput(13, out)
        && bitput(m_state.lenBits, dv >> suffix);
}

//===========================================================================
bool DbPack::bitput(size_t nbits, uint64_t value) {
    assert(nbits >= 0 && nbits <= 64);
    assert(nbits == 64 || value < (1ull << nbits));
    if (!bitcheck(nbits))
        return false;

    auto cnt = nbits;
    for (;;) {
        if (!m_unusedBits) {
            if (m_count == m_used)
                return false;
            m_base[m_used++] = 0;
            m_unusedBits = 8;
        }

        if (m_unusedBits >= cnt) {
            auto bits = value & ((1 << cnt) - 1);
            bits <<= m_unusedBits - cnt;
            m_base[m_used - 1] |= bits;
            m_unusedBits -= (uint8_t) cnt;
            break;
        }

        auto bits = value >> (cnt - m_unusedBits);
        bits &= (1 << m_unusedBits) - 1;
        m_base[m_used - 1] |= bits;
        cnt -= m_unusedBits;
        m_unusedBits = 0;
    }
    return true;
}

//===========================================================================
bool DbPack::bitcheck(size_t nbits) {
    auto space = 8 * (m_count - m_used) + unusedBits();
    return nbits <= space;
}


/****************************************************************************
*
*   DbUnpackIter
*
***/

//===========================================================================
DbUnpackIter::DbUnpackIter(void const * src, size_t srcLen, size_t unusedBits)
    : m_base{(unsigned char *) src}
    , m_count{srcLen}
    , m_trailingUnused{(uint8_t) unusedBits}
{
    operator++();
}

//===========================================================================
bool DbUnpackIter::operator!=(DbUnpackIter const & right) const {
    return m_base == right.m_base
        && m_used == right.m_used
        && m_unusedBits == right.m_unusedBits;
}

//===========================================================================
DbUnpackIter & DbUnpackIter::operator++() {
    if (!getTime() || !getValue())
        *this = {};
    return *this;
}

//===========================================================================
bool DbUnpackIter::getTime() {
    int64_t s;
    uint64_t u;
    if (!bitget(&u, 1))
        return false;
    if (!u) {
        // '0' - delta same as previous delta
        m_state.sample.time += m_state.dt;
        return true;
    }
    if (!bitget(&u, 1))
        return false;
    if (!u) {
        // '10' + ddt (7 bits)
        if (!bitget(&s, 7))
            return false;
    } else {
        if (!bitget(&u, 1))
            return false;
        if (!u) {
            // '110' + ddt (12 bits)
            if (!bitget(&s, 12))
                return false;
        } else {
            if (!bitget(&u, 1))
                return false;
            if (!u) {
                // '1110' + ddt (n bits, depending on exponent)
                if (!bitget(&s, kExponentInfo[m_state.expBits].bits))
                    return false;
            } else {
                // '1111' + exponent (3 bits)
                if (!bitget(&u, 3))
                    return false;
                m_state.expBits = (uint8_t) u;
                return getTime();
            }
        }
    }
    if (s >= 0)
        s += 1;
    auto odt = Duration{s * kExponentInfo[m_state.expBits].factor};
    m_state.dt += odt;
    m_state.sample.time += m_state.dt;
    return true;
}

//===========================================================================
bool DbUnpackIter::getValue() {
    uint64_t out;
    if (!bitget(&out, 1))
        return false;
    if (!out) {
        // '0'
        return true;
    }
    if (!bitget(&out, 1))
        return false;
    if (out) {
        // '11' + leading zeros (5 bits) + xor length (6 bits) + xor (number of
        //      bits given by length)
        if (!bitget(&out, 5))
            return false;
        m_state.prefixBits = (uint8_t) out;
        if (!bitget(&out, 6))
            return false;
        m_state.lenBits = (uint8_t) out;
    } else {
        // '10' + xor (use current leading zero and length values)
    }

    if (!bitget(&out, m_state.lenBits))
        return false;
    (uint64_t &) m_state.sample.value ^=
        out << (64 - m_state.lenBits - m_state.prefixBits);
    return true;
}

//===========================================================================
bool DbUnpackIter::bitget(int64_t * out, size_t nbits) {
    if (!bitget((uint64_t *) out, nbits))
        return false;
    if (nbits && (*out & (1ull << (nbits - 1))) && nbits < 64)
        *out |= -1 << nbits;
    return true;
}

//===========================================================================
bool DbUnpackIter::bitget(uint64_t * out, size_t nbits) {
    assert(nbits > 0 && nbits <= 64);
    auto availBits = 8 * (m_count - m_used) + m_unusedBits - m_trailingUnused;
    if (availBits < nbits)
        return false;
    *out = 0;
    auto cnt = nbits;
    for (;;) {
        if (!m_unusedBits) {
            assert(m_count != m_used);
            m_used += 1;
            m_unusedBits = 8;
        }
        if (cnt <= m_unusedBits) {
            auto bits = m_base[m_used - 1] >> (m_unusedBits - cnt);
            bits &= ((1 << cnt) - 1);
            *out |= bits;
            m_unusedBits -= (uint8_t) cnt;
            break;
        }

        auto bits = m_base[m_used - 1] & ((1 << m_unusedBits) - 1);
        bits <<= cnt - m_unusedBits;
        *out |= bits;
        cnt -= m_unusedBits;
        m_unusedBits = 0;
    }
    return true;
}
