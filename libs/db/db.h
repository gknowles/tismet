// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// Database of time series metrics
//
// db.h - tismet db
#pragma once

#include "core/core.h"
#include "file/file.h"

#include <limits>
#include <string_view>

// forward declarations
struct IDbDataNotify;


/****************************************************************************
*
*   Open, close, general configuration, and status commands
*
***/

struct DbHandle : Dim::HandleBase {};

enum DbOpenFlags : unsigned {
    fDbOpenCreat = 0x01,
    fDbOpenTrunc = 0x02,
    fDbOpenExcl = 0x04,

    // Log database status info messages
    fDbOpenVerbose = 0x08,
    fDbOpenReadOnly = 0x10,
};
DbHandle dbOpen(
    std::string_view path,
    size_t pageSize = 0, // 0 for same size as system memory pages
    DbOpenFlags flags = {}
);

void dbClose(DbHandle h);

// Setting a parameter to zero causes that specific parameter to be unchanged.
struct DbConfig {
    Dim::Duration checkpointMaxInterval;
    size_t checkpointMaxData;
};
void dbConfigure(DbHandle h, const DbConfig & conf);

struct DbStats {
    // Constant for life of database
    unsigned pageSize;
    unsigned segmentSize;
    unsigned metricNameSize; // includes terminating null

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

// Returns true if it completed synchronously
bool dbGetMetricInfo(
    IDbDataNotify * notify,
    DbHandle h,
    uint32_t id
);

// returns true if inserted, false if it already existed, sets out either way
bool dbInsertMetric(uint32_t * out, DbHandle h, std::string_view name);

void dbEraseMetric(DbHandle h, uint32_t id);

struct DbMetricInfo {
    Dim::Duration retention{};
    Dim::TimePoint creation;
};
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
    bool infoEx{false};
    uint32_t id{}; // for metrics, the metric id, otherwise 0
    std::string_view target; // query series is from, empty for metrics
    std::string_view name; // such as metric name or alias
    Dim::TimePoint first;
    Dim::TimePoint last; // time of first interval after the end
    Dim::Duration interval{};
};
// Used in callback from dbGetMetricInfo()
struct DbSeriesInfoEx : DbSeriesInfo {
    DbSeriesInfoEx() { infoEx = true; }
    Dim::Duration retention{};
    Dim::TimePoint creation;
};
struct IDbDataNotify {
    virtual ~IDbDataNotify() = default;

    // Called once before any calls to onDbSample, return false to abort the
    // enum, otherwise it continues to the samples.
    virtual bool onDbSeriesStart(const DbSeriesInfo & info) { return true; }

    virtual void onDbSeriesEnd(uint32_t id) {}

    // Called for each matching sample, return false to abort the enum,
    // otherwise it continues to the next sample.
    virtual bool onDbSample(
        uint32_t id,
        Dim::TimePoint time,
        double value
    ) { return false; }
};
// Returns true if it completed synchronously, false if the request was
// queued.
bool dbGetSamples(
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
    virtual ~IDbProgressNotify() = default;
    virtual bool onDbProgress(
        Dim::RunMode mode,
        const DbProgressInfo & info
    ) = 0;
};

// returns false if backup is already running
bool dbBackup(IDbProgressNotify * notify, DbHandle h, std::string_view dst);

void dbBlockCheckpoint(IDbProgressNotify * notify, DbHandle h, bool enable);


/****************************************************************************
*
*   Internals for special utility programs
*
***/

enum pgno_t : uint32_t {};

enum class DbPageType : int32_t {
    kInvalid = 0,
    kFree = 'F',
    kZero = 'dZ',
    kSegment = 'S',
    kRadix = 'r',
    kIndexBranch = 'bi',
    kIndexLeaf = 'li',
    kMetric = 'm',
    kSample = 's',
};

enum DbPageFlags : uint32_t {
    fDbPageDirty = 1,
};

struct DbPageHeader {
    DbPageType type;
    pgno_t pgno;
    uint32_t id;
    uint32_t checksum;
    uint64_t lsn;
};
