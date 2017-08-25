// Copyright Glen Knowles 2017.
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
    QueryInfo * m_query;
    std::vector<QueryInfo::Node *> m_nodes;

    const char * m_start{nullptr};
    const char * m_end{nullptr};

    bool m_minus{false};
    int64_t m_int{0};
    int m_frac{0};
    bool m_expMinus{false};
    int m_exp{0};

    unsigned char m_charStart{0};
    std::bitset<256> m_chars;

    // Functions
    QueryParserBase(QueryInfo * qry) : m_query{qry} {}

    bool startFunc(QueryInfo::NodeType type);
};
