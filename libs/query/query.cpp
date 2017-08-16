// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// query.cpp - tismet query
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kQueryMaxSize = 8192;


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
bool queryParse(QueryInfo & qry, std::string_view src) {
    return false;
}
