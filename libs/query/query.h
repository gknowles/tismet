// Copyright Glen Knowles 2017 - 2021.
// Distributed under the Boost Software License, Version 1.0.
//
// query.h - tismet query
#pragma once

#include "cppconf/cppconf.h"

#include "core/core.h"
#include "querydefs/querydefs.h"

#include <bitset>
#include <memory>
#include <string_view>
#include <vector>


/****************************************************************************
*
*   Public API
*
***/

namespace Query {

// Returns false on malformed input, otherwise true and the query was
// successfully parsed.
bool parse(
    QueryInfo & qry,
    std::string_view src,
    const ITokenConvNotify * notify
);

} // namespace
