// Copyright Glen Knowles 2023 - 2025.
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
DbPack::DbPack(void * out, size_t outBytes) {
    retarget(out, outBytes);
}

//===========================================================================
DbPack::DbPack(
    void * out,
    size_t outBytes,
    size_t bitPos,
    const DbPackState & st
) {
    retarget(out, outBytes, bitPos, st);
}

//===========================================================================
DbUnpackIter DbPack::find(
    size_t bitPos,
    const DbPackState & state
) const {
    return DbUnpackIter(data(), bits(), bitPos, state);
}

//===========================================================================
void DbPack::retarget(void * out, size_t outBytes) {
    m_base = (unsigned char *) out;
    m_bytes = outBytes;
}

//===========================================================================
void DbPack::retarget(size_t bitPos, const DbPackState & st) {
    assert(bitPos < m_bytes * 8);
    m_samplePos = bitPos;
    m_sampleBits = 0;
    m_state = st;
}

//===========================================================================
void DbPack::retarget(
    void * out,
    size_t outBytes,
    size_t bitPos,
    const DbPackState & st
) {
    retarget(out, outBytes);
    retarget(bitPos, st);
}

//===========================================================================
bool DbPack::put(TimePoint time, double value) {
    m_samplePos += m_sampleBits;
    m_sampleBits = 0;
    return put(time) && put(value);
}

//===========================================================================
bool DbPack::put(TimePoint time) {
    assert(!m_samplePos || time > m_state.sample.time);
    auto dt = time - m_state.sample.time;
    auto ddt = (dt - m_state.dt).count();
    m_state.sample.time = time;
    m_state.dt = dt;
    if (ddt == 0) {
        // Same as previous time.
        // '0'
        return putUint(1, 0);
    }
    if (ddt % kExponentInfo[m_state.expBits].factor) {
        // Too small for previous exponent, record new exponent.
        // '1111' + exponent (3 bits)
        do {
            m_state.expBits -= 1;
        } while (ddt % kExponentInfo[m_state.expBits].factor);
        if (!putUint(7, kExponentInfo[m_state.expBits].encoded))
            return false;
    }
    ddt /= kExponentInfo[m_state.expBits].factor;
    if (ddt < 0) {
        if (ddt >= -64) {
            // ddt within [-64, -1]
            // '10' + ddt (7 bits)
            return availBits() >= 2 + 7
                && putUint(2, 0b10)
                && putInt(7, ddt);
        } else if (ddt >= -2048) {
            // ddt within [-2048, -65]
            // '110' + ddt (12 bits)
            return availBits() >= 3 + 12
                && putUint(3, 0b110)
                && putInt(12, ddt);
        } else {
            auto bits = kExponentInfo[m_state.expBits].bits;
            if (bits < 60) {
                // ddt within [-2^59, -2049]
                // '1110' + ddt (41 - 58 bits, depending on exponent)
                return availBits() >= 4 + bits
                    && putUint(4, 0b1110)
                    && putInt(bits, ddt);
            } else {
                // ddt within [-2^63, -2049]
                // '1110' + ddt (61 - 64 bits, depending on exponent)
                return availBits() >= 4 + bits
                    && putUint(4, 0b1110)
                    && putInt(bits, ddt);
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
            return availBits() >= 2 + 7
                && putUint(2, 0b10)
                && putInt(7, ddt);
        } else if (ddt <= 2047) {
            // ddt within [65, 2048]
            // '110' + (ddt - 1) (12 bits)
            return availBits() >= 3 + 12
                && putUint(3, 0b110)
                && putInt(12, ddt);
        } else {
            auto bits = kExponentInfo[m_state.expBits].bits;
            if (bits < 60) {
                // ddt within [2049, 2^59]
                // '1110' + (ddt - 1) (41 - 58 bits, depending on exponent)
                return availBits() >= 4 + bits
                    && putUint(4, 0b1110)
                    && putInt(bits, ddt);
            } else {
                // ddt within [2049, 2^59]
                // '1110' + (ddt - 1) (61 - 64 bits, depending on exponent)
                return availBits() >= 4 + bits
                    && putUint(4, 0b1110)
                    && putInt(bits, ddt);
            }
        }
    }
}

//===========================================================================
bool DbPack::put(double value) {
    auto dv = bit_cast<uint64_t>(value)
        ^ bit_cast<uint64_t>(m_state.sample.value);
    m_state.sample.value = value;
    if (!dv) {
        // Same as previous value.
        // '0'
        return availBits() >= 1 && putUint(1, 0);
    }

    auto prefix = min(countl_zero(dv), 31);
    auto len = 64 - prefix - countr_zero(dv);
    if (prefix >= m_state.prefixBits
        && prefix + len <= m_state.prefixBits + m_state.lenBits
    ) {
        // Meaningful bits (i.e. not the leading or trailing zeros) fits
        // within previous range.
        // '10' + meaningful bits
        auto suffix = 64 - m_state.prefixBits - m_state.lenBits;
        return availBits() >= 2 + m_state.lenBits
            && putUint(2, 0b10)
            && putUint(m_state.lenBits, dv >> suffix);
    }

    // Specify new range of meaningful bits as well as the new value.
    // '11' + number of leading zeros (5 bits)
    //      + number of meaningful bits (6 bits)
    //      + meaningful bits
    m_state.prefixBits = (uint8_t) prefix;
    m_state.lenBits = (uint8_t) len;
    auto out = (0b11 << 11) | (m_state.prefixBits << 6) | m_state.lenBits;
    auto suffix = 64 - m_state.prefixBits - m_state.lenBits;
    return availBits() >= 13 + m_state.lenBits
        && putUint(13, out)
        && putUint(m_state.lenBits, dv >> suffix);
}

//===========================================================================
// 'nbits' includes space for leading sign bit.
bool DbPack::putInt(size_t nbits, int64_t value) {
    assert(nbits > 1 && nbits <= 64);
    assert(availBits() >= nbits);
    if (value < 0) {
        uint64_t val = -value;
        assert(val < ((uint64_t) 1 << (nbits - 1)));
        val = ((uint64_t) 1 << (nbits - 1)) | val;
        return putUint(nbits, val);
    } else {
        assert(value < ((int64_t) 1 << (nbits - 1)));
        return putUint(nbits, value);
    }
}

//===========================================================================
bool DbPack::putUint(size_t nbits, uint64_t value) {
    assert(nbits > 0 && nbits <= 64);
    assert(nbits == 64 || value < (1ull << nbits));
    assert(availBits() >= nbits);

    auto pos = m_samplePos + m_sampleBits;
    auto cnt = nbits;
    for (;;) {
        auto used = pos / 8;
        auto unusedBits = 8 - (pos % 8);
        if (!unusedBits)
            m_base[used] = 0;

        if (unusedBits >= cnt) {
            auto bits = value & ((1 << cnt) - 1);
            bits <<= unusedBits - cnt;
            m_base[used] |= bits;
            pos += cnt;
            break;
        }

        auto bits = value >> (cnt - unusedBits);
        bits &= (1 << unusedBits) - 1;
        m_base[used] |= bits;
        cnt -= unusedBits;
        pos += unusedBits;
    }
    m_sampleBits = pos - m_samplePos;
    return true;
}

//===========================================================================
size_t DbPack::availBits() {
    return 8 * capacity() - bits();
}


/****************************************************************************
*
*   DbUnpackIter
*
***/

//===========================================================================
DbUnpackIter::DbUnpackIter(
    const void * src,
    size_t srcBits,
    size_t bitPos,
    const DbPackState & st
)
    : m_base{(unsigned char *) src}
    , m_bits{srcBits}
{
    seek(bitPos, st);
}

//===========================================================================
DbUnpackIter::operator bool() const {
    return bits() != m_samplePos;
}

//===========================================================================
bool DbUnpackIter::operator==(const DbUnpackIter & right) const {
    return !*this && !right
        || m_base == right.m_base
            && m_samplePos == right.m_samplePos
            && bits() == right.bits();
}

//===========================================================================
DbUnpackIter & DbUnpackIter::operator++() {
    m_samplePos += m_sampleBits;
    m_sampleBits = 0;
    if (!getTime() || !getValue()) {
        m_samplePos = bits();
        m_sampleBits = 0;
    }
    return *this;
}

//===========================================================================
void DbUnpackIter::seek(size_t bitPos, const DbPackState & state) {
    assert(bitPos <= bits());
    m_samplePos = bitPos;
    m_sampleBits = 0;
    m_state = state;
    operator++();
}

//===========================================================================
bool DbUnpackIter::getTime() {
    int64_t s;
    uint64_t u;
    if (!getUint(&u, 1))
        return false;
    if (!u) {
        // '0' - delta same as previous delta
        assert(!m_samplePos || m_state.dt.count() > 0);
        m_state.sample.time += m_state.dt;
        return true;
    }
    if (!getUint(&u, 1))
        return false;
    if (!u) {
        // '10' + ddt (7 bits)
        if (!getInt(&s, 7))
            return false;
    } else {
        if (!getUint(&u, 1))
            return false;
        if (!u) {
            // '110' + ddt (12 bits)
            if (!getInt(&s, 12))
                return false;
        } else {
            if (!getUint(&u, 1))
                return false;
            if (!u) {
                // '1110' + ddt (n bits, depending on exponent)
                if (!getInt(&s, kExponentInfo[m_state.expBits].bits))
                    return false;
            } else {
                // '1111' + exponent (3 bits)
                if (!getUint(&u, 3))
                    return false;
                m_state.expBits = (uint8_t) u;
                return getTime();
            }
        }
    }
    if (s >= 0)
        s += 1;
    auto ddt = Duration{s * kExponentInfo[m_state.expBits].factor};
    m_state.dt += ddt;
    assert(!m_samplePos || m_state.dt.count() > 0);
    m_state.sample.time += m_state.dt;
    return true;
}

//===========================================================================
bool DbUnpackIter::getValue() {
    uint64_t out;

    if (!getUint(&out, 1))
        return false;
    if (!out) {
        // '0'
        return true;
    }
    if (!getUint(&out, 1))
        return false;
    if (!out) {
        // '10' + xor (use current leading zero and length values)
    } else {
        // '11' + leading zeros (5 bits) + xor length (6 bits) + xor (number of
        //      bits given by length)
        if (!getUint(&out, 5))
            return false;
        m_state.prefixBits = (uint8_t) out;
        if (!getUint(&out, 6))
            return false;
        m_state.lenBits = (uint8_t) out;
    }

    if (!getUint(&out, m_state.lenBits))
        return false;
    static_assert(sizeof (uint64_t) == sizeof m_state.sample.value);
    (uint64_t &) m_state.sample.value ^=
        out << (64 - m_state.lenBits - m_state.prefixBits);
    return true;
}

//===========================================================================
bool DbUnpackIter::getInt(int64_t * out, size_t nbits) {
    if (!getUint((uint64_t *) out, nbits))
        return false;
    if (nbits > 1 && nbits < 64) {
        auto signbit = *out & int64_t(1ull << (nbits - 1));
        if (signbit)
            *out = signbit - *out;
    }
    return true;
}

//===========================================================================
bool DbUnpackIter::getUint(uint64_t * out, size_t nbits) {
    assert(nbits > 0 && nbits <= 64);
    auto pos = m_samplePos + m_sampleBits;
    auto availBits = bits() - pos;
    if (availBits < nbits)
        return false;
    *out = 0;
    auto cnt = nbits;
    for (;;) {
        auto used = pos / 8;
        auto unusedBits = 8 - (pos % 8);
        if (cnt <= unusedBits) {
            auto bits = m_base[used] >> (unusedBits - cnt);
            bits &= ((1 << cnt) - 1);
            *out <<= cnt;
            *out |= bits;
            pos += cnt;
            break;
        }

        auto bits = m_base[used] & ((1 << unusedBits) - 1);
        *out <<= unusedBits;
        *out |= bits;
        cnt -= unusedBits;
        pos += unusedBits;
    }
    m_sampleBits = pos - m_samplePos;
    return true;
}
