// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// Database of time series metrics
//
// db.h - tismet db
#pragma once

#include "core/core.h"
#include "file/file.h"


/****************************************************************************
*
*   Open, close, general configuration, and status commands
*
***/

struct DbHandle : Dim::HandleBase {};

enum DbOpenFlags : unsigned {
    // Log database status info messages
    fDbOpenVerbose = 1,
};
DbHandle dbOpen(
    std::string_view path,
    size_t pageSize = 0, // 0 for same size as system memory pages
    DbOpenFlags flags = {}
);

void dbClose(DbHandle h);

// Setting a parameter to zero causes that specific parameter to be unchanged.
struct DbConfig {
    Dim::Duration pageMaxAge;
    Dim::Duration pageScanInterval;

    Dim::Duration checkpointMaxInterval;
    size_t checkpointMaxData;
};
void dbConfigure(DbHandle h, const DbConfig & conf);

enum DbSampleType : int8_t {
    kSampleTypeInvalid = 0,
    kSampleTypeFloat32 = 1,
    kSampleTypeFloat64 = 2,
    kSampleTypeInt8    = 3,
    kSampleTypeInt16   = 4,
    kSampleTypeInt32   = 5,
    kSampleTypes,
};
const char * toString(DbSampleType type, const char def[] = nullptr);
DbSampleType fromString(std::string_view src, DbSampleType def);

struct DbStats {
    // Constant for life of database
    unsigned pageSize;
    unsigned segmentSize;
    unsigned metricNameSize; // includes terminating null
    unsigned samplesPerPage[kSampleTypes];

    // Changes as data is modified
    unsigned numPages;
    unsigned freePages;
    unsigned metrics;
};
DbStats dbQueryStats(DbHandle h);


/****************************************************************************
*
*   Metric series
*
***/

// returns true if found
bool dbFindMetric(uint32_t & out, DbHandle h, std::string_view name);

void dbFindMetrics(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view pattern = {}  // empty name for all
);
const char * dbGetMetricName(DbHandle h, uint32_t id);

struct MetricInfo {
    std::string_view name;
    DbSampleType type{kSampleTypeInvalid};
    Dim::Duration retention;
    Dim::Duration interval;
    Dim::TimePoint first;
};
bool dbGetMetricInfo(
    MetricInfo & info,
    DbHandle h,
    uint32_t id
);

// returns true if inserted, false if it already existed, sets out either way
bool dbInsertMetric(uint32_t & out, DbHandle h, std::string_view name);

void dbEraseMetric(DbHandle h, uint32_t id);

// Removes all existing data when type, retention, or interval are changed.
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    const MetricInfo & info
);

// returns all branches containing metrics that match the pattern
void dbFindBranches(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view pattern = {}  // empty name for all
);
const char * dbGetBranchName(DbHandle h, uint32_t branchId);


/****************************************************************************
*
*   Samples
*
***/

void dbUpdateSample(
    DbHandle h,
    uint32_t id,
    Dim::TimePoint time,
    double value
);

struct IDbEnumNotify {
    virtual ~IDbEnumNotify() {}

    // Called once before any calls to OnDbSample, return false to abort the
    // enum, otherwise it continues to the samples.
    virtual bool OnDbMetricStart(
        uint32_t id,
        std::string_view name,
        DbSampleType type,
        Dim::TimePoint from,
        Dim::TimePoint until,
        Dim::Duration interval
    ) { return true; }

    virtual void OnDbMetricEnd(uint32_t id) {}

    // Called for each matching sample, return false to abort the enum,
    // otherwise it continues to the next sample.
    virtual bool OnDbSample(Dim::TimePoint time, double value) { return false; }
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
*   Database dump and backup
*
***/

struct DbProgressInfo {
    size_t metrics{0};
    size_t totalMetrics{(size_t) -1};    // -1 for unknown
    size_t samples{0};
    size_t totalSamples{(size_t) -1};
    size_t bytes{0};
    size_t totalBytes{(size_t) -1};
    size_t files{0};
    size_t totalFiles{(size_t) -1};
};
struct IDbProgressNotify {
    virtual ~IDbProgressNotify() {}
    virtual bool OnDbProgress(
        Dim::RunMode mode,
        const DbProgressInfo & info
    ) = 0;
};

// returns false if backup is already running
bool dbBackup(IDbProgressNotify * notify, DbHandle h, std::string_view dst);

void dbBlockCheckpoint(IDbProgressNotify * notify, DbHandle h, bool enable);

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
