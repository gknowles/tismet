// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.h - tismet
#pragma once

#include "core/core.h"


/****************************************************************************
*
*   Declarations
*
***/

struct TsdFileHandle : Dim::HandleBase {};

TsdFileHandle tsdOpen(std::string_view name, size_t pageSize = 0);

void tsdClose(TsdFileHandle file);

bool tsdFindMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
void tsdEraseMetric(TsdFileHandle h, uint32_t id);

// Removes all existing data when retention or interval are changed.
void tsdUpdateMetric(
    TsdFileHandle h,
    uint32_t id,
    Dim::Duration retention,
    Dim::Duration interval
);

void tsdWriteData(
    TsdFileHandle h, 
    uint32_t id, 
    Dim::TimePoint time, 
    float value
);

void tsdDump(std::ostream & os, TsdFileHandle file);
