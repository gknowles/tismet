// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// carbonparsebaseint.h - tismet carbon
#pragma once

#include <cstdint>


/****************************************************************************
*
*   CarbonParserBase
*
***/

struct CarbonParserBase {
    CarbonUpdate * m_upd;

    const char * m_nameStart{nullptr};
    const char * m_nameEnd{nullptr};
    bool m_minus{false};
    int64_t m_int{0};
    int m_frac{0};
    bool m_expMinus{false};
    int m_exp{0};
    uint64_t m_seconds{0};

    CarbonParserBase(CarbonUpdate * upd) : m_upd{upd} {}
};
