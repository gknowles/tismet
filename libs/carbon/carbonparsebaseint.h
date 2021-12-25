// Copyright Glen Knowles 2017 - 2021.
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

    const char * m_nameStart{};
    const char * m_nameEnd{};
    bool m_minus{};
    int64_t m_int{};
    int m_frac{};
    bool m_expMinus{};
    int m_exp{};
    int64_t m_seconds{};

    CarbonParserBase(CarbonUpdate * upd) : m_upd{upd} {}
};
