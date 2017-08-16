// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// queryparsebaseint.h - tismet query
#pragma once

#include <cstdint>


/****************************************************************************
*
*   QueryParserBase
*
***/

struct QueryParserBase {
    QueryInfo * m_query;

    const char * m_nameStart{nullptr};
    const char * m_nameEnd{nullptr};
    bool m_minus{false};
    int64_t m_int{0};
    int m_frac{0};
    bool m_expMinus{false};
    int m_exp{0};
    uint64_t m_seconds{0};

    QueryParserBase(QueryInfo * qry) : m_query{qry} {}
};
