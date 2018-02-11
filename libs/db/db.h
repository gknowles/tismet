// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// Database of time series metrics
//
// db.h - tismet db
#pragma once

#include "core/core.h"
#include "file/file.h"

// forward declarations
struct IDbDataNotify;


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

// Metric context prevents metric ids from changing their meaning (i.e. being
// reassigned to different metrics) during the life of the context.
struct DbContextHandle : Dim::HandleBase {};

DbContextHandle dbOpenContext(DbHandle h);
void dbCloseContext(DbContextHandle h);

// returns true if found
bool dbFindMetric(uint32_t * out, DbHandle h, std::string_view name);

void dbFindMetrics(
    Dim::UnsignedSet * out,
    DbHandle h,
    std::string_view pattern = {}  // empty name for all
);
const char * dbGetMetricName(DbHandle h, uint32_t id);

void dbGetMetricInfo(
    IDbDataNotify * notify,
    DbHandle h,
    uint32_t id
);

// returns true if inserted, false if it already existed, sets out either way
bool dbInsertMetric(uint32_t * out, DbHandle h, std::string_view name);

void dbEraseMetric(DbHandle h, uint32_t id);

struct DbMetricInfo {
    std::string_view name;
    DbSampleType type{kSampleTypeInvalid};
    Dim::Duration retention;
    Dim::Duration interval;
};
// Removes all existing data when type, retention, or interval are changed.
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    const DbMetricInfo & info
);

// returns all branches containing metrics that match the pattern
void dbFindBranches(
    Dim::UnsignedSet * out,
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

struct DbSeriesInfo {
    std::string_view target; // query series is from, empty for metrics
    uint32_t id{0}; // for metrics, the metric id, otherwise 0
    std::string_view name; // such as metric name or alias
    DbSampleType type{kSampleTypeInvalid};
    Dim::TimePoint first;
    Dim::TimePoint last; // time of first interval after the end
    Dim::Duration interval;
};
struct IDbDataNotify {
    virtual ~IDbDataNotify() = default;

    // Called once before any calls to onDbSample, return false to abort the
    // enum, otherwise it continues to the samples.
    virtual bool onDbSeriesStart(const DbSeriesInfo & info) { return true; }

    virtual void onDbSeriesEnd(uint32_t id) {}

    // Called for each matching sample, return false to abort the enum,
    // otherwise it continues to the next sample.
    virtual bool onDbSample(Dim::TimePoint time, double value) { return false; }
};
void dbGetSamples(
    IDbDataNotify * notify,
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
    virtual bool onDbProgress(
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
