// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.h - tismet
#pragma once

#include "core/handle.h"


/****************************************************************************
*
*   Declarations
*
***/

struct TsdFileHandle : Dim::HandleBase {};

TsdFileHandle tsdOpen(std::string_view name);

void tsdClose(TsdFileHandle file);

bool tsdFindMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
