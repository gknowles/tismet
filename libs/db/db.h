// Copyright Glen Knowles 2017 - 2023.
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
// 'pageSize' is only used if new files are being created, use 0 for the same
// size as system memory pages.
DbHandle dbOpen(
    std::string_view path,
    Dim::EnumFlags<DbOpenFlags> flags = {},
    size_t pageSize = 0
);

void dbClose(DbHandle h);

// Setting a parameter to zero causes that specific parameter to be unchanged.
struct DbConfig {
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
    unsigned bitsPerPage;
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
class DbContext : public Dim::NoCopy {
public:
    DbContext() noexcept = default;
    DbContext(DbHandle f);
    ~DbContext();

    DbHandle handle() const;
    void reset(DbHandle f = {});

private:
    DbHandle m_f;
    uint64_t m_instance{0};
};

// Returns true if found.
bool dbFindMetric(uint32_t * out, DbHandle h, std::string_view name);

void dbFindMetrics(
    Dim::UnsignedSet * out,
    DbHandle h,
    std::string_view pattern = {}  // empty name for all
);
const char * dbGetMetricName(DbHandle h, uint32_t id);

// Returns true if it completed synchronously.
bool dbGetMetricInfo(
    IDbDataNotify * notify,
    DbHandle h,
    uint32_t id
);

// Returns true if inserted, false if it already existed, sets out either way.
bool dbInsertMetric(uint32_t * out, DbHandle h, std::string_view name);

void dbEraseMetric(DbHandle h, uint32_t id);

struct DbMetricInfo {
    std::string_view name;
    DbSampleType type{kSampleTypeInvalid};
    Dim::Duration retention{};
    Dim::Duration interval{};
    Dim::TimePoint creation;
};
// Removes all existing data when type, retention, or interval are changed.
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    const DbMetricInfo & info
);

// Returns all branches containing metrics that match the pattern.
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
    DbSampleType type{kSampleTypeInvalid};
    uint32_t id{0}; // for metrics, the metric id, otherwise 0
    std::string_view target; // query series is from, empty for metrics
    std::string_view name; // such as metric name or alias
    Dim::TimePoint first;
    Dim::TimePoint last; // time of first interval after the end
    Dim::Duration interval{};
};
// Used in callback from dbGetMetricInfo().
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
    ) {
        return false;
    }
};
// Returns true if it completed synchronously, false if the request was queued.
bool dbGetSamples(
    IDbDataNotify * notify,
    DbHandle h,
    uint32_t id,
    Dim::TimePoint first = {},
    Dim::TimePoint last = Dim::TimePoint::max(),
    unsigned presamples = 0
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

// Returns false if backup is already running.
bool dbBackup(IDbProgressNotify * notify, DbHandle h, std::string_view dst);

void dbBlockCheckpoint(IDbProgressNotify * notify, DbHandle h, bool enable);


/****************************************************************************
*
*   Internals for special utility programs
*
***/

enum pgno_t : uint32_t { npos = std::numeric_limits<uint32_t>::max() };

enum LocalTxn : uint16_t {};

struct Lsn {
    uint64_t val : 48;

    bool operator==(const Lsn & b) const = default;
    std::strong_ordering operator<=>(const Lsn & b) const = default;
    explicit operator bool() const { return val; }
    Lsn & operator+=(ptrdiff_t b) { val += b; return *this; }
    Lsn & operator-=(ptrdiff_t b) { val -= b; return *this; }
    Lsn & operator++() { val += 1; return *this; }
    Lsn & operator--() { val -= 1; return *this; }
    Lsn operator++(int) { return Lsn(val + 1); }
    Lsn operator--(int) { return Lsn(val - 1); }
};
static_assert(sizeof Lsn == 8);
inline Lsn operator+(Lsn a, ptrdiff_t b) { return a += b; }
inline Lsn operator+(ptrdiff_t a, Lsn b) { return b += a; }
inline Lsn operator-(Lsn a, ptrdiff_t b) { return a -= b; }
inline ptrdiff_t operator-(const Lsn & a, const Lsn & b) {
    return a.val - b.val;
}
inline std::ostream & operator<<(std::ostream & os, const Lsn & lsn) {
    os << lsn.val;
    return os;
}

struct Lsx {
    uint64_t localTxn : 16;
    uint64_t lsn : 48;

    bool operator==(const Lsx & other) const = default;
    std::strong_ordering operator<=>(const Lsx & other) const = default;
    explicit operator bool() const { return lsn || localTxn; }
    explicit operator Lsn() const { return Lsn(lsn); }
};
static_assert(sizeof Lsx == 8);
template<> struct std::hash<Lsx> {
    size_t operator()(const Lsx & val) const {
        auto out = std::hash<uint64_t>()(val.localTxn);
        Dim::hashCombine(&out, std::hash<uint64_t>()(val.lsn));
        return out;
    }
};

enum class DbPageType : int32_t {
    kInvalid = 0,
    kFree = 'F',
    kZero = 'dZ',
    kMetric = 'm',
    kRadix = 'r',
    kSample = 's',
    kTrie = 't',
    kBitmap = 'b',
};
std::string toString(DbPageType type);

enum DbPageFlags : uint32_t {
    fDbPageDirty = 1,
};

#pragma pack(push, 1)

struct DbPageHeader {
    DbPageType type;
    pgno_t pgno;
    uint32_t id;
    uint32_t checksum;
    Lsn lsn;
};

#pragma pack(pop)
