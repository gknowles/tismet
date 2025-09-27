// Copyright Glen Knowles 2023 - 2025.
// Distributed under the Boost Software License, Version 1.0.
//
// dbpack.h - tismet db
#pragma once

#include "cppconf/cppconf.h"

#include "core/time.h"

#include <cstdint>
#include <string_view>


/****************************************************************************
*
*   Pack & Unpack
*
***/

struct DbSample {
    Dim::TimePoint time;
    double value;

    bool operator==(const DbSample &) const = default;
};

struct DbPackState {
    DbSample sample{};
    Dim::Duration dt{};
    uint8_t expBits{7};
    uint8_t prefixBits{31};
    uint8_t lenBits{};

    bool operator==(const DbPackState &) const = default;
};


/****************************************************************************
*
*   DbUnpackIter
*
***/

class DbUnpackIter {
public:
    DbUnpackIter() {}
    DbUnpackIter(
        const void * src,
        size_t srcBits,
        size_t bitPos = 0,
        const DbPackState & state = {}
    );
    explicit operator bool() const;
    bool operator==(const DbUnpackIter & right) const;
    DbUnpackIter & operator++();
    DbSample & operator*() { return m_state.sample; }
    DbSample * operator->() { return &m_state.sample; }

    const unsigned char * data() const { return m_base; }
    size_t size() const { return (m_bits + 7) / 8; }
    std::string_view view() const { return {(char *) data(), size()}; }
    size_t bits() const { return m_bits; }
    const DbPackState & state() const { return m_state; }
    size_t spos() const { return m_samplePos; }
    size_t slen() const { return m_sampleBits; }

    void seek(size_t bitPos, const DbPackState & state);

private:
    bool getInt(int64_t * out, size_t nbits);
    bool getUint(uint64_t * out, size_t nbits);
    bool getTime();
    bool getValue();

    // Source
    const uint8_t * m_base{};
    size_t m_bits{};

    // Position
    size_t m_samplePos{};
    size_t m_sampleBits{};

    // State
    DbPackState m_state;
};

inline DbUnpackIter begin(DbUnpackIter iter) { return iter; }
inline DbUnpackIter end(const DbUnpackIter & iter) { return {}; }


/****************************************************************************
*
*   DbPack
*
***/

class DbPack {
public:
    DbPack(void * out, size_t outLen);
    DbPack(
        void * out,
        size_t outLen,
        size_t bitPos,
        const DbPackState & st
    );

    void retarget(void * out, size_t outLen);
    void retarget(
        void * out,
        size_t outLen,
        size_t bitPos,
        const DbPackState & st
    );
    bool put(Dim::TimePoint time, double value);
    bool put(const DbSample & s) { return put(s.time, s.value); }

    uint8_t * data() const { return m_base; }
    size_t size() const { return (bits() + 7) / 8; }
    std::span<uint8_t> span() const { return {data(), size()}; }
    size_t bits() const { return m_samplePos + m_sampleBits; }
    size_t capacity() const { return m_count; }
    const DbPackState & state() const { return m_state; }

    DbUnpackIter begin() const { return find(); }
    DbUnpackIter end() const { return {}; }
    DbUnpackIter find(size_t bitPos = 0, const DbPackState & state = {}) const;

private:
    bool putInt(size_t nbits, int64_t value);
    bool putUint(size_t nbits, uint64_t value);
    size_t availBits();
    bool put(Dim::TimePoint time);
    bool put(double value);

    // Target
    uint8_t * m_base{};
    size_t m_count{};

    // Position
    size_t m_samplePos{};
    size_t m_sampleBits{};

    // State
    DbPackState m_state;
};
