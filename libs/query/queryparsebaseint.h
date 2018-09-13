// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// queryparsebaseint.h - tismet query
#pragma once

#include <bitset>
#include <cstdint>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   QueryParserBase
*
***/

struct QueryParserBase {
    Query::QueryInfo * m_query;
    std::vector<Query::Node *> m_nodes;

    const char * m_start{};
    const char * m_end{};

    bool m_pathSeg{}; // true if pathSeg just ended

    bool m_minus{};
    int64_t m_int{};
    int m_frac{};
    bool m_expMinus{};
    int m_exp{};

    unsigned char m_charStart{};
    std::bitset<256> m_chars;

    // Functions
    QueryParserBase(Query::QueryInfo * qry) : m_query{qry} {}

    bool startFunc(Eval::Function::Type type);
};
