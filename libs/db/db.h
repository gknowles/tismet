// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// db.h - tismet db
#pragma once

#include "core/core.h"
#include "file/file.h"


/****************************************************************************
*
*   Database of time series metrics
*
***/

struct DbHandle : Dim::HandleBase {};

DbHandle dbOpen(std::string_view name, size_t pageSize = 0);

void dbClose(DbHandle h);

// Setting a parameter to zero causes that specific parameter to be unchanged.
struct DbConfig {
    Dim::Duration pageMaxAge;
    Dim::Duration pageScanInterval;

    Dim::Duration checkpointMaxInterval;
    size_t checkpointMaxData;
};
void dbConfigure(DbHandle h, const DbConfig & conf);

struct DbStats {
    // Constant for life of database
    unsigned pageSize;
    unsigned segmentSize;
    unsigned metricNameLength; // includes terminating null
    unsigned samplesPerPage;

    // Change as data is modified
    unsigned numPages;
    unsigned freePages;
    unsigned metrics;
};
DbStats dbQueryStats(DbHandle h);

// returns true if found
bool dbFindMetric(uint32_t & out, DbHandle h, std::string_view name);

void dbFindMetrics(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view wildcardName = {}  // empty name for all
);

const char * dbGetMetricName(DbHandle h, uint32_t id);

// returns true if inserted, false if it already existed, sets out either way
bool dbInsertMetric(uint32_t & out, DbHandle h, std::string_view name);

void dbEraseMetric(DbHandle h, uint32_t id);

struct MetricInfo {
    Dim::Duration retention;
    Dim::Duration interval;
};
bool dbGetMetricInfo(
    MetricInfo & info,
    DbHandle h,
    uint32_t id
);
// Removes all existing data when retention or interval are changed.
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    const MetricInfo & info
);

void dbUpdateSample(
    DbHandle h,
    uint32_t id,
    Dim::TimePoint time,
    float value
);

struct IDbEnumNotify {
    virtual ~IDbEnumNotify() {}
    // Called for each matching sample, return false to abort the enum,
    // otherwise it continues to the next sample.
    virtual bool OnDbSample(
        uint32_t id,
        std::string_view name,
        Dim::TimePoint time,
        float value
    ) = 0;
};
size_t dbEnumSamples(
    IDbEnumNotify * notify,
    DbHandle h,
    uint32_t id,
    Dim::TimePoint first = {},
    Dim::TimePoint last = Dim::TimePoint::max()
);


/****************************************************************************
*
*   Database dump files
*
***/

struct DbProgressInfo {
    size_t metrics{0};
    size_t totalMetrics{(size_t) -1};    // -1 for unknown
    size_t samples{0};
    size_t totalValues{(size_t) -1};
    size_t bytes{0};
    size_t totalBytes{(size_t) -1};
};
struct IDbProgressNotify {
    virtual ~IDbProgressNotify() {}
    virtual bool OnDbProgress(
        bool complete,
        const DbProgressInfo & info
    ) = 0;
};

void dbWriteDump(
    IDbProgressNotify * notify,
    std::ostream & os,
    DbHandle h,
    std::string_view wildname = {}
);

void dbLoadDump(
    IDbProgressNotify * notify,
    DbHandle h,
    const Dim::Path & src
);
