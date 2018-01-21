// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// db.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

namespace {

class DbBase
    : public HandleContent
    , IDbEnumNotify
    , IDbProgressNotify
    , IFileReadNotify
{
public:
    DbBase();

    bool open(string_view name, size_t pageSize, DbOpenFlags flags);
    void configure(const DbConfig & conf);
    DbStats queryStats();
    void blockCheckpoint(IDbProgressNotify * notify, bool enable);
    bool backup(IDbProgressNotify * notify, string_view dst);

    bool insertMetric(uint32_t & out, string_view name);
    void eraseMetric(uint32_t id);
    void updateMetric(
        uint32_t id,
        const MetricInfo & info
    );

    const char * getMetricName(uint32_t id) const;
    bool getMetricInfo(MetricInfo & info, uint32_t id) const;

    bool findMetric(uint32_t & out, string_view name) const;
    void findMetrics(UnsignedSet & out, string_view pattern) const;

    const char * getBranchName(uint32_t id) const;
    void findBranches(UnsignedSet & out, string_view pattern) const;

    void updateSample(uint32_t id, TimePoint time, double value);
    size_t enumSamples(
        IDbEnumNotify * notify,
        uint32_t id,
        TimePoint first,
        TimePoint last
    );

private:
    // Inherited via IDbEnumNotify
    void OnDbMetric(
        uint32_t id,
        string_view vname,
        DbSampleType type,
        TimePoint from,
        TimePoint until,
        Duration interval
    ) override;

    // Inherited via IDbProgressNotify
    bool OnDbProgress(RunMode mode, const DbProgressInfo & info) override;

    void backupNextFile();

    // Inherited via IFileReadNotify
    bool onFileRead(
        size_t * bytesUsed,
        string_view data,
        int64_t offset,
        FileHandle f
    ) override;
    void onFileEnd(int64_t offset, FileHandle f) override;

    RunMode m_backupMode{kRunStopped};
    DbProgressInfo m_info;
    IDbProgressNotify * m_backer{nullptr};
    vector<pair<Path, Path>> m_backupFiles;
    FileAppendQueue m_dstFile;

    DbIndex m_leaf;
    DbIndex m_branch;

    DbPage m_page;
    DbData m_data;
    DbLog m_log; // MUST be last! (and destroyed first)
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<DbHandle, DbBase> s_files;

static auto & s_perfCreated = uperf("db metrics created");
static auto & s_perfDeleted = uperf("db metrics deleted");


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   DbBase
*
***/

//===========================================================================
DbBase::DbBase ()
    : m_dstFile(100, 2, envMemoryConfig().pageSize)
    , m_log(m_data, m_page)
{}

//===========================================================================
bool DbBase::open(string_view name, size_t pageSize, DbOpenFlags flags) {
    auto datafile = Path(name).setExt("tsd");
    auto workfile = Path(name).setExt("tsw");
    auto logfile = Path(name).setExt("tsl");
    if (!m_page.open(datafile, workfile, pageSize, flags))
        return false;
    m_data.openForApply(m_page.pageSize(), flags);
    if (!m_log.open(logfile, flags))
        return false;
    DbTxn txn{m_log, m_page};
    return m_data.openForUpdate(txn, this, datafile, flags);
}

//===========================================================================
void DbBase::OnDbMetric(
    uint32_t id,
    string_view name,
    DbSampleType type,
    TimePoint from,
    TimePoint until,
    Duration interval
) {
    m_leaf.insert(id, name);
    m_branch.insertBranches(name);
}

//===========================================================================
void DbBase::configure(const DbConfig & conf) {
    m_page.configure(conf);
    m_log.configure(conf);
}

//===========================================================================
DbStats DbBase::queryStats() {
    return m_data.queryStats();
}

//===========================================================================
void DbBase::blockCheckpoint(IDbProgressNotify * notify, bool enable) {
    m_log.blockCheckpoint(notify, enable);
}


/****************************************************************************
*
*   Backup
*
***/

//===========================================================================
bool DbBase::backup(IDbProgressNotify * notify, string_view dstStem) {
    if (m_backupMode != kRunStopped)
        return false;

    m_backupFiles.clear();
    Path src = filePath(m_page.dataFile());
    Path dst = dstStem;
    dst.setExt(src.extension());
    m_backupFiles.push_back(make_pair(dst, src));
    src = filePath(m_log.logFile());
    dst.setExt(src.extension());
    m_backupFiles.push_back(make_pair(dst, src));
    m_backer = notify;
    m_backupMode = kRunStarting;
    m_info = {};
    m_info.totalFiles = m_backupFiles.size();
    blockCheckpoint(this, true);
    return true;
}

//===========================================================================
bool DbBase::OnDbProgress(RunMode mode, const DbProgressInfo & info) {
    if (m_backupMode != kRunStarting)
        return true;

    if (mode == kRunStopped) {
        m_backupMode = kRunRunning;
        if (m_backer && !m_backer->OnDbProgress(m_backupMode, m_info)) {
            m_backupFiles.clear();
        }
        backupNextFile();
        return true;
    }

    assert(mode == kRunStopping);
    return m_backer ? m_backer->OnDbProgress(m_backupMode, m_info) : true;
}

//===========================================================================
void DbBase::backupNextFile() {
    if (!m_backupFiles.empty()) {
        auto [dst, src] = m_backupFiles.front();
        if (m_dstFile.open(dst, FileAppendQueue::kTrunc)) {
            if (m_info.totalBytes == size_t(-1)) {
                m_info.totalBytes = fileSize(src);
            } else {
                m_info.totalBytes += fileSize(src);
            }
            m_backupFiles.erase(m_backupFiles.begin());
            fileStreamBinary(this, src, 65536, taskComputeQueue());
            return;
        }
        logMsgError() << "Unable to create " << dst;
        m_backupFiles.clear();
    }

    blockCheckpoint(this, false);
    if (m_backer)
        m_backer->OnDbProgress(kRunStopped, m_info);
    m_backupMode = kRunStopped;
}

//===========================================================================
bool DbBase::onFileRead(
    size_t * bytesUsed,
    string_view data,
    int64_t offset,
    FileHandle f
) {
    *bytesUsed = data.size();
    m_info.bytes += *bytesUsed;
    m_dstFile.append(data);
    if (m_backer && !m_backer->OnDbProgress(m_backupMode, m_info)) {
        m_backupMode = kRunStopping;
        return false;
    }
    return true;
}

//===========================================================================
void DbBase::onFileEnd(int64_t offset, FileHandle f) {
    m_dstFile.close();
    if (m_backupMode == kRunStopping) {
        m_backupFiles.clear();
    } else {
        m_info.files += 1;
    }
    backupNextFile();
}


/****************************************************************************
*
*   Metrics
*
***/

//===========================================================================
bool DbBase::insertMetric(uint32_t & out, string_view name) {
    if (findMetric(out, name))
        return false;

    // get metric id
    out = m_leaf.nextId();

    // update indexes
    m_leaf.insert(out, name);
    m_branch.insertBranches(name);

    // set info page
    DbTxn txn{m_log, m_page};
    m_data.insertMetric(txn, out, name);
    s_perfCreated += 1;
    return true;
}

//===========================================================================
void DbBase::eraseMetric(uint32_t id) {
    DbTxn txn{m_log, m_page};
    string name;
    if (m_data.eraseMetric(txn, name, id)) {
        m_leaf.erase(name);
        m_branch.eraseBranches(name);
        s_perfDeleted += 1;
    }
}

//===========================================================================
void DbBase::updateMetric(uint32_t id, const MetricInfo & info) {
    DbTxn txn{m_log, m_page};
    m_data.updateMetric(txn, id, info);
}

//===========================================================================
const char * DbBase::getMetricName(uint32_t id) const {
    return m_leaf.name(id);
}

//===========================================================================
bool DbBase::getMetricInfo(MetricInfo & info, uint32_t id) const {
    auto self = const_cast<DbBase *>(this);
    DbTxn txn{self->m_log, self->m_page};
    return m_data.getMetricInfo(txn, info, id);
}

//===========================================================================
bool DbBase::findMetric(uint32_t & out, string_view name) const {
    return m_leaf.find(out, name);
}

//===========================================================================
void DbBase::findMetrics(UnsignedSet & out, string_view pattern) const {
    m_leaf.find(out, pattern);
}

//===========================================================================
const char * DbBase::getBranchName(uint32_t id) const {
    return m_branch.name(id);
}

//===========================================================================
void DbBase::findBranches(UnsignedSet & out, string_view pattern) const {
    m_branch.find(out, pattern);
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
void DbBase::updateSample(uint32_t id, TimePoint time, double value) {
    DbTxn txn{m_log, m_page};
    m_data.updateSample(txn, id, time, value);
}

//===========================================================================
size_t DbBase::enumSamples(
    IDbEnumNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    DbTxn txn{m_log, m_page};
    return m_data.enumSamples(txn, notify, id, first, last);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
DbHandle dbOpen(string_view name, size_t pageSize, DbOpenFlags flags) {
    auto db = make_unique<DbBase>();
    if (!db->open(name, pageSize, flags))
        return DbHandle{};

    auto h = s_files.insert(db.release());
    return h;
}

//===========================================================================
void dbClose(DbHandle h) {
    s_files.erase(h);
}

static TokenTable::Token s_sampleTypes[] = {
    { kSampleTypeFloat32,   "float32" },
    { kSampleTypeFloat64,   "float64" },
    { kSampleTypeInt8,      "int8" },
    { kSampleTypeInt16,     "int16" },
    { kSampleTypeInt32,     "int32" },
};
static_assert(size(s_sampleTypes) == kSampleTypes - 1);
static TokenTable s_sampleTypeTbl{s_sampleTypes, size(s_sampleTypes)};

//===========================================================================
const char * toString(DbSampleType type, const char def[]) {
    return tokenTableGetName(s_sampleTypeTbl, type, def);
}

//===========================================================================
DbSampleType fromString(std::string_view src, DbSampleType def) {
    return tokenTableGetEnum(s_sampleTypeTbl, src, def);
}

//===========================================================================
void dbConfigure(DbHandle h, const DbConfig & conf) {
    s_files.find(h)->configure(conf);
}

//===========================================================================
DbStats dbQueryStats(DbHandle h) {
    return s_files.find(h)->queryStats();
}

//===========================================================================
void dbBlockCheckpoint(IDbProgressNotify * notify, DbHandle h, bool enable) {
    s_files.find(h)->blockCheckpoint(notify, enable);
}

//===========================================================================
bool dbBackup(IDbProgressNotify * notify, DbHandle h, string_view dst) {
    return s_files.find(h)->backup(notify, dst);
}

//===========================================================================
bool dbInsertMetric(uint32_t & out, DbHandle h, string_view name) {
    return s_files.find(h)->insertMetric(out, name);
}

//===========================================================================
void dbEraseMetric(DbHandle h, uint32_t id) {
    s_files.find(h)->eraseMetric(id);
}

//===========================================================================
void dbUpdateMetric(DbHandle h, uint32_t id, const MetricInfo & info) {
    s_files.find(h)->updateMetric(id, info);
}

//===========================================================================
const char * dbGetMetricName(DbHandle h, uint32_t id) {
    return s_files.find(h)->getMetricName(id);
}

//===========================================================================
bool dbGetMetricInfo(MetricInfo & info, DbHandle h, uint32_t id) {
    return s_files.find(h)->getMetricInfo(info, id);
}

//===========================================================================
bool dbFindMetric(uint32_t & out, DbHandle h, string_view name) {
    return s_files.find(h)->findMetric(out, name);
}

//===========================================================================
void dbFindMetrics(UnsignedSet & out, DbHandle h, string_view name) {
    s_files.find(h)->findMetrics(out, name);
}

//===========================================================================
const char * dbGetBranchName(DbHandle h, uint32_t id) {
    return s_files.find(h)->getBranchName(id);
}

//===========================================================================
void dbFindBranches(UnsignedSet & out, DbHandle h, string_view name) {
    s_files.find(h)->findBranches(out, name);
}

//===========================================================================
void dbUpdateSample(DbHandle h, uint32_t id, TimePoint time, double value) {
    s_files.find(h)->updateSample(id, time, value);
}

//===========================================================================
size_t dbEnumSamples(
    IDbEnumNotify * notify,
    DbHandle h,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    return s_files.find(h)->enumSamples(notify, id, first, last);
}
