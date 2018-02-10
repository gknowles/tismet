// Copyright Glen Knowles 2017 - 2018.
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

enum DbReqType {
    kGetMetric,
    kGetSamples,
    kEraseMetric,
    kInsertMetric,
    kUpdateMetric,
    kUpdateSample,
};
struct DbReq {
    DbReqType type;
    string name;
    DbSampleType sampleType;
    Duration retention;
    Duration interval;
    IDbDataNotify * notify;
    TimePoint first;
    TimePoint last;
    double value;
};

class DbBase
    : public HandleContent
    , IDbDataNotify
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

    uint64_t acquireInstanceRef();
    void releaseInstanceRef(uint64_t instance);

    bool insertMetric(uint32_t * out, string_view name);
    void eraseMetric(uint32_t id);
    void updateMetric(
        uint32_t id,
        const DbMetricInfo & info
    );

    const char * getMetricName(uint32_t id) const;
    void getMetricInfo(IDbDataNotify * notify, uint32_t id) const;

    bool findMetric(uint32_t * out, string_view name) const;
    void findMetrics(UnsignedSet * out, string_view pattern) const;

    const char * getBranchName(uint32_t id) const;
    void findBranches(UnsignedSet * out, string_view pattern) const;

    void updateSample(uint32_t id, TimePoint time, double value);
    void getSamples(
        IDbDataNotify * notify,
        uint32_t id,
        TimePoint first,
        TimePoint last
    );

private:
    void transact(uint32_t id, DbReq && req);

    // Inherited via IDbDataNotify
    bool onDbSeriesStart(const DbSeriesInfo & info) override;

    // Inherited via IDbProgressNotify
    bool onDbProgress(RunMode mode, const DbProgressInfo & info) override;

    void backupNextFile();

    // Inherited via IFileReadNotify
    bool onFileRead(
        size_t * bytesUsed,
        string_view data,
        int64_t offset,
        FileHandle f
    ) override;
    void onFileEnd(int64_t offset, FileHandle f) override;

    mutex m_dataMut;
    unordered_map<uint32_t, deque<DbReq>> m_requests;
    bool m_verbose{false};

    RunMode m_backupMode{kRunStopped};
    DbProgressInfo m_info;
    IDbProgressNotify * m_backer{nullptr};
    vector<pair<Path, Path>> m_backupFiles;
    FileAppendQueue m_dstFile;

    mutable shared_mutex m_indexMut;
    uint64_t m_instance{0};
    DbIndex m_leaf;
    DbIndex m_branch;

    DbPage m_page;
    DbData m_data;
    DbLog m_log; // MUST be last! (and destroyed first)
};

class DbContext : public HandleContent {
public:
    DbContext(DbHandle f);
    ~DbContext();

    DbHandle handle() const;
    DbBase * db() const;

private:
    DbHandle m_f;
    uint64_t m_instance{0};
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<DbHandle, DbBase> s_files;
static HandleMap<DbContextHandle, DbContext> s_contexts;

static auto & s_perfCreated = uperf("db metrics created");
static auto & s_perfDeleted = uperf("db metrics deleted");


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
inline static DbBase * db(DbContextHandle h) {
    return s_contexts.find(h)->db();
}

//===========================================================================
inline static DbBase * db(DbHandle h) {
    return s_files.find(h);
}


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
    m_verbose = flags & fDbOpenVerbose;

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
bool DbBase::onDbSeriesStart(const DbSeriesInfo & info) {
    m_leaf.insert(info.id, info.name);
    m_branch.insertBranches(info.name);
    return true;
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

    if (m_verbose)
        logMsgInfo() << "Backup started";
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
bool DbBase::onDbProgress(RunMode mode, const DbProgressInfo & info) {
    if (m_backupMode != kRunStarting)
        return true;

    if (mode == kRunStopped) {
        m_backupMode = kRunRunning;
        if (m_backer && !m_backer->onDbProgress(m_backupMode, m_info)) {
            m_backupFiles.clear();
        }
        backupNextFile();
        return true;
    }

    assert(mode == kRunStopping);
    return m_backer ? m_backer->onDbProgress(m_backupMode, m_info) : true;
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
        m_backer->onDbProgress(kRunStopped, m_info);
    m_backupMode = kRunStopped;
    if (m_verbose)
        logMsgInfo() << "Backup completed";
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
    if (m_backer && !m_backer->onDbProgress(m_backupMode, m_info)) {
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
*   Contexts
*
***/

//===========================================================================
DbContextHandle::operator DbHandle() const {
    return s_contexts.find(*this)->handle();
}

//===========================================================================
DbContext::DbContext(DbHandle f)
    : m_f{f}
{
    if (m_f)
        m_instance = db()->acquireInstanceRef();
}

//===========================================================================
DbContext::~DbContext() {
    if (m_f)
        db()->releaseInstanceRef(m_instance);
}

//===========================================================================
DbHandle DbContext::handle() const {
    return m_f;
}

//===========================================================================
DbBase * DbContext::db() const {
    return s_files.find(m_f);
}


/****************************************************************************
*
*   Transactions
*
***/

//===========================================================================
void DbBase::transact(uint32_t id, DbReq && req) {
    unique_lock<mutex> lk{m_dataMut};
    auto & reqs = m_requests[id];
    reqs.push_back(move(req));
    if (reqs.size() != 1)
        return;

    while (!reqs.empty()) {
        req = move(reqs.front());
        lk.unlock();

        DbTxn txn{m_log, m_page};
        switch (req.type) {
        case kGetMetric:
            m_data.getMetricInfo(req.notify, txn, id);
            break;
        case kGetSamples:
            m_data.getSamples(txn, req.notify, id, req.first, req.last);
            break;
        case kEraseMetric:
            if (m_data.eraseMetric(&req.name, txn, id)) {
                scoped_lock<shared_mutex> lk{m_indexMut};
                m_leaf.erase(req.name);
                m_branch.eraseBranches(req.name);
                s_perfDeleted += 1;
            }
            break;
        case kInsertMetric:
            m_data.insertMetric(txn, id, req.name);
            s_perfCreated += 1;
            break;
        case kUpdateMetric:
            {
                DbMetricInfo info;
                info.type = req.sampleType;
                info.retention = req.retention;
                info.interval = req.interval;
                m_data.updateMetric(txn, id, info);
            }
            break;
        case kUpdateSample:
            m_data.updateSample(txn, id, req.first, req.value);
            break;
        }

        lk.lock();
        reqs.pop_front();
    }
}


/****************************************************************************
*
*   Metrics
*
***/

//===========================================================================
uint64_t DbBase::acquireInstanceRef() {
    scoped_lock<shared_mutex> lk{m_indexMut};
    return m_leaf.acquireInstanceRef();
}

//===========================================================================
void DbBase::releaseInstanceRef(uint64_t instance) {
    scoped_lock<shared_mutex> lk{m_indexMut};
    m_leaf.releaseInstanceRef(instance);
}

//===========================================================================
bool DbBase::insertMetric(uint32_t * out, string_view name) {
    {
        shared_lock<shared_mutex> lk{m_indexMut};
        if (m_leaf.find(out, name))
            return false;
    }

    {
        scoped_lock<shared_mutex> lk{m_indexMut};
        if (m_leaf.find(out, name))
            return false;

        // get metric id
        *out = m_leaf.nextId();

        // update indexes
        m_leaf.insert(*out, name);
        m_branch.insertBranches(name);
    }

    // set info page
    DbReq req;
    req.type = kInsertMetric;
    req.name = name;
    transact(*out, move(req));
    return true;
}

//===========================================================================
void DbBase::eraseMetric(uint32_t id) {
    DbReq req;
    req.type = kEraseMetric;
    transact(id, move(req));
}

//===========================================================================
void DbBase::updateMetric(uint32_t id, const DbMetricInfo & info) {
    DbReq req;
    req.type = kUpdateMetric;
    req.sampleType = info.type;
    req.retention = info.retention;
    req.interval = info.interval;
    transact(id, move(req));
}

//===========================================================================
const char * DbBase::getMetricName(uint32_t id) const {
    shared_lock<shared_mutex> lk{m_indexMut};
    return m_leaf.name(id);
}

//===========================================================================
void DbBase::getMetricInfo(IDbDataNotify * notify, uint32_t id) const {
    auto self = const_cast<DbBase *>(this);
    DbReq req;
    req.type = kGetMetric;
    req.notify = notify;
    self->transact(id, move(req));
}

//===========================================================================
bool DbBase::findMetric(uint32_t * out, string_view name) const {
    shared_lock<shared_mutex> lk{m_indexMut};
    return m_leaf.find(out, name);
}

//===========================================================================
void DbBase::findMetrics(UnsignedSet * out, string_view pattern) const {
    shared_lock<shared_mutex> lk{m_indexMut};
    m_leaf.find(out, pattern);
}

//===========================================================================
const char * DbBase::getBranchName(uint32_t id) const {
    shared_lock<shared_mutex> lk{m_indexMut};
    return m_branch.name(id);
}

//===========================================================================
void DbBase::findBranches(UnsignedSet * out, string_view pattern) const {
    shared_lock<shared_mutex> lk{m_indexMut};
    m_branch.find(out, pattern);
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
void DbBase::updateSample(uint32_t id, TimePoint time, double value) {
    DbReq req;
    req.type = kUpdateSample;
    req.first = time;
    req.value = value;
    transact(id, move(req));
}

//===========================================================================
void DbBase::getSamples(
    IDbDataNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    DbReq req;
    req.type = kGetSamples;
    req.notify = notify;
    req.first = first;
    req.last = last;
    transact(id, move(req));
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
DbContextHandle dbOpenContext(DbHandle f) {
    if (!s_files.find(f)) {
        return {};
    } else {
        return s_contexts.insert(new DbContext(f));
    }
}

//===========================================================================
void dbCloseContext(DbContextHandle h) {
    s_contexts.erase(h);
}

//===========================================================================
bool dbInsertMetric(uint32_t * out, DbHandle h, string_view name) {
    return db(h)->insertMetric(out, name);
}

//===========================================================================
void dbEraseMetric(DbHandle h, uint32_t id) {
    db(h)->eraseMetric(id);
}

//===========================================================================
void dbUpdateMetric(DbHandle h, uint32_t id, const DbMetricInfo & info) {
    db(h)->updateMetric(id, info);
}

//===========================================================================
const char * dbGetMetricName(DbHandle h, uint32_t id) {
    return db(h)->getMetricName(id);
}

//===========================================================================
void dbGetMetricInfo(IDbDataNotify * notify, DbHandle h, uint32_t id) {
    db(h)->getMetricInfo(notify, id);
}

//===========================================================================
bool dbFindMetric(uint32_t * out, DbHandle h, string_view name) {
    return db(h)->findMetric(out, name);
}

//===========================================================================
void dbFindMetrics(UnsignedSet * out, DbHandle h, string_view name) {
    db(h)->findMetrics(out, name);
}

//===========================================================================
const char * dbGetBranchName(DbHandle h, uint32_t id) {
    return db(h)->getBranchName(id);
}

//===========================================================================
void dbFindBranches(UnsignedSet * out, DbHandle h, string_view name) {
    db(h)->findBranches(out, name);
}

//===========================================================================
void dbUpdateSample(
    DbHandle h,
    uint32_t id,
    TimePoint time,
    double value
) {
    db(h)->updateSample(id, time, value);
}

//===========================================================================
void dbGetSamples(
    IDbDataNotify * notify,
    DbHandle h,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    db(h)->getSamples(notify, id, first, last);
}
