// Copyright Glen Knowles 2023.
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
};

struct DbPackState {
    DbSample sample{};
    Dim::Duration dt{};
    uint8_t expBits{7};
    uint8_t prefixBits{31};
    uint8_t lenBits{};
};


/****************************************************************************
*
*   DbPack
*
***/

class DbUnpackIter;

class DbPack {
public:
    DbPack(void * out, size_t outLen, size_t unusedBits = 0);
    DbPack(const DbUnpackIter & unpack);

    void retarget(void * out, size_t outLen, size_t unusedBits = 0);
    bool put(Dim::TimePoint time, double value);

    const unsigned char * data() const { return m_base; }
    size_t size() const { return m_used; }
    std::string_view view() const { return {(char *) m_base, m_used}; }
    uint8_t unusedBits() const { return m_unusedBits; }
    size_t capacity() const { return m_count; }

private:
    bool bitput(size_t nbits, uint64_t value);
    bool bitcheck(size_t nbits);
    bool put(Dim::TimePoint time);
    bool put(double value);

    // Target
    unsigned char * m_base{};
    size_t m_count{};

    // Position
    size_t m_used{};
    uint8_t m_unusedBits{0};

    // State
    DbPackState m_state;
};


/****************************************************************************
*
*   DbUnpack
*
***/

class DbUnpackIter {
public:
    DbUnpackIter() {}
    DbUnpackIter(const void * src, size_t srcLen, size_t unusedBits);
    explicit operator bool() const { return m_base; }
    bool operator!=(const DbUnpackIter & right) const;
    DbUnpackIter & operator++();
    DbSample & operator*() { return m_state.sample; }
    DbSample * operator->() { return &m_state.sample; }

    const unsigned char * data() const { return m_base; }
    size_t size() const { return m_count; }
    std::string_view view() const { return {(char *) data(), size()}; }
    uint8_t unusedBits() const { return m_unusedBits; }

    const DbPackState & state() const { return m_state; }

private:
    bool bitget(int64_t * out, size_t nbits);
    bool bitget(uint64_t * out, size_t nbits);
    bool getTime();
    bool getValue();

    // Source
    const unsigned char * m_base{};
    size_t m_count{};
    uint8_t m_trailingUnused{};

    // Position
    size_t m_used{};
    uint8_t m_unusedBits{0};

    // State
    DbPackState m_state;
};

inline DbUnpackIter begin(DbUnpackIter iter) { return iter; }
inline DbUnpackIter end(const DbUnpackIter & iter) { return {}; }
