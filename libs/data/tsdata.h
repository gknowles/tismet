// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tsdata.h - tismet data
#pragma once

#include "core/core.h"
#include "file/file.h"


/****************************************************************************
*
*   Declarations
*
***/

struct TsdFileHandle : Dim::HandleBase {};

TsdFileHandle tsdOpen(std::string_view name, size_t pageSize = 0);

void tsdClose(TsdFileHandle h);

// returns true if found
bool tsdFindMetric(uint32_t & out, TsdFileHandle h, std::string_view name);

// returns true if inserted, false if it already existed, sets out either way
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, std::string_view name);

void tsdEraseMetric(TsdFileHandle h, uint32_t id);

// Removes all existing data when retention or interval are changed.
void tsdUpdateMetric(
    TsdFileHandle h,
    uint32_t id,
    Dim::Duration retention,
    Dim::Duration interval
);

void tsdUpdateValue(
    TsdFileHandle h, 
    uint32_t id, 
    Dim::TimePoint time, 
    float value
);

void tsdFindMetrics(
    Dim::UnsignedSet & out,
    TsdFileHandle h,
    std::string_view wildcardName = {}  // empty name for all
);

struct ITsdEnumNotify {
    virtual ~ITsdEnumNotify() {}
    // Called for each matching value, return false to abort the enum, 
    // otherwise it continues to the next value.
    virtual bool OnTsdValue(
        uint32_t id,
        std::string_view name,
        Dim::TimePoint time,
        float value
    ) = 0;
};
size_t tsdEnumValues(
    ITsdEnumNotify * notify,
    TsdFileHandle h,
    uint32_t id,
    Dim::TimePoint first = {},
    Dim::TimePoint last = Dim::TimePoint::max()
);

void tsdWriteDump(
    std::ostream & os, 
    TsdFileHandle h, 
    std::string_view wildname = {}
);

struct TsdProgressInfo {
    size_t metrics{0};
    size_t totalMetrics{(size_t) -1};    // -1 for unknown
    size_t values{0};
    size_t totalValues{(size_t) -1};
    size_t bytes{0};
    size_t totalBytes{(size_t) -1};
};
struct ITsdProgressNotify {
    virtual ~ITsdProgressNotify() {}
    virtual bool OnTsdProgress(
        bool complete, 
        const TsdProgressInfo & info
    ) = 0;
};
void tsdLoadDump(
    ITsdProgressNotify * notify,
    TsdFileHandle h,
    const Dim::Path & src
);
