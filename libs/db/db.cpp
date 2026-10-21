// Copyright Glen Knowles 2017 - 2026.
// Distributed under the Boost Software License, Version 1.0.
//
// db.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kRequestBuckets = 8;


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
    unsigned presamples;
    double value;
};

} // namespace

class DbBase
    : public HandleContent
    , IDbDataNotify
    , IDbProgressNotify
    , IFileReadNotify
{
public:
    DbBase();

    bool open(
        string_view name,
        EnumFlags<DbOpenFlags> flags,
        size_t pageSize
    );
    void close();
    void configure(const DbConfig & conf);
    DbInfo queryInfo() const;
    DbStats queryStats() const;
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
    bool getMetricInfo(IDbDataNotify * notify, uint32_t id) const;

    bool findMetric(uint32_t * out, string_view name) const;
    void findMetrics(UnsignedSet * out, string_view pattern) const;

    const char * getBranchName(uint32_t id) const;
    void findBranches(UnsignedSet * out, string_view pattern) const;

    void updateSample(uint32_t id, TimePoint time, double value);
    bool getSamples(
        IDbDataNotify * notify,
        uint32_t id,
        TimePoint first,
        TimePoint last,
        unsigned presamples
    );

private:
    // Returns true if it completed synchronously
    bool transact(uint32_t id, DbReq && req);
    void apply(uint32_t id, DbReq && req);

    // Inherited via IDbDataNotify
    bool onDbSeriesStart(const DbSeriesInfo & info) override;

    // Inherited via IDbProgressNotify
    bool onDbProgress(RunMode mode, const DbProgressInfo & info) override;

    void backupNextFile();

    // Inherited via IFileReadNotify
    bool onFileRead(size_t * bytesUsed, const FileReadData & data) override;

    struct RequestBucket {
        mutex mut;
        unordered_map<uint32_t, deque<DbReq>> requests;
    };
    unique_ptr<RequestBucket[]> m_reqBuckets;
    bool m_verbose{false};

    // Backup process
    RunMode m_backupMode{kRunStopped};
    DbProgressInfo m_info;
    IDbProgressNotify * m_backer{};
    vector<pair<Path, Path>> m_backupFiles;
    FileAppendStream m_dstFile;

    // Metric name search
    mutable shared_mutex m_indexMut;
    uint64_t m_instance{};
    DbIndex m_leaf;
    DbIndex m_branch;

    // Persistent data
    DbPage m_page;
    DbData m_data;
    DbWal m_wal; // MUST be last! (and destroyed first)
};


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfCreated = uperf("db.metrics created");
static auto & s_perfDeleted = uperf("db.metrics deleted");
static auto & s_perfTrunc = uperf("db.metric names truncated");


/****************************************************************************
*
*   DbBase
*
***/

//===========================================================================
DbBase::DbBase ()
    : m_dstFile(100, 2, envMemoryConfig().pageSize)
    , m_wal(&m_data, &m_page)
{
    m_reqBuckets.reset(new RequestBucket[kRequestBuckets]);
}

//===========================================================================
bool DbBase::open(
    string_view name,
    EnumFlags<DbOpenFlags> flags,
    size_t pageSize
) {
    m_verbose = flags.any(fDbOpenVerbose);

    auto datafile = Path(name).setExt("tsd");
    auto workfile = Path(name).setExt("tsw");
    auto walfile = Path(name).setExt("tsl");
    if (!m_wal.open(walfile, flags, pageSize))
        return false;
    if (!m_wal.newFiles())
        flags.reset(fDbOpenNew | fDbOpenAlways);
    if (!m_page.open(
        datafile,
        workfile,
        m_wal.dataPageSize(),
        m_wal.walPageSize(),
        flags
    )) {
        m_wal.close();
        return false;
    }
    m_data.openForApply(m_page.pageSize(), flags);
    if (!m_wal.recover())
        return false;
    DbTxn txn{m_wal, m_page, m_data.metricRootsInstance()};
    if (!m_data.openForUpdate(txn, this, datafile, flags))
        return false;
    auto freePages = txn.commit();
    m_data.publishFreePages(freePages);
    m_wal.checkpoint();
    return true;
}

//===========================================================================
void DbBase::close() {
    m_wal.close();
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
    m_wal.configure(conf);
}

//===========================================================================
DbInfo DbBase::queryInfo() const {
    DbInfo out;
    out.datafile = filePath(m_page.dataFile());
    out.workfile = filePath(m_page.workFile());
    out.walfile = filePath(m_wal.walFile());
    out.flags = m_page.openFlags();
    out.newFiles = m_page.newFiles();
    return out;
}

//===========================================================================
DbStats DbBase::queryStats() const {
    return m_data.queryStats();
}

//===========================================================================
void DbBase::blockCheckpoint(IDbProgressNotify * notify, bool enable) {
    m_wal.blockCheckpoint(notify, enable);
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
    auto src = (Path) filePath(m_page.dataFile());
    auto dst = (Path) dstStem;
    dst.setExt(src.extension());
    m_backupFiles.push_back(make_pair(dst, src));
    src = filePath(m_wal.walFile());
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
        if (m_dstFile.open(dst, FileAppendStream::kTrunc)) {
            uint64_t bytes;
            fileSize(&bytes, src);
            if (m_info.totalBytes == size_t(-1)) {
                m_info.totalBytes = bytes;
            } else {
                m_info.totalBytes += bytes;
            }
            m_backupFiles.erase(m_backupFiles.begin());
            fileStreamBinary(this, src, 65536, taskComputeQueue());
            return;
        }
        logMsgError() << "Create failed, " << dst;
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
bool DbBase::onFileRead(size_t * bytesUsed, const FileReadData & data) {
    auto more = data.more;
    *bytesUsed = data.data.size();
    m_info.bytes += *bytesUsed;
    m_dstFile.append(data.data);
    if (m_backer && !m_backer->onDbProgress(m_backupMode, m_info)) {
        m_backupMode = kRunStopping;
        m_backupFiles.clear();
        more = false;
    }
    if (!more) {
        m_dstFile.close();
        if (m_backupMode != kRunStopping)
            m_info.files += 1;
        backupNextFile();
    }
    return more;
}


/****************************************************************************
*
*   Contexts
*
***/

//===========================================================================
DbContext::DbContext(DbBase * db) {
    reset(db);
}

//===========================================================================
DbContext::~DbContext() {
    reset();
}

//===========================================================================
DbBase * DbContext::db() const {
    return m_db;
}

//===========================================================================
void DbContext::reset(DbBase * db) {
    if (!db && !m_instance) {
        m_db = db;
        return;
    }

    if (m_db && m_instance)
        m_db->releaseInstanceRef(m_instance);
    m_db = db;
    m_instance = 0;
    if (db)
        m_instance = db->acquireInstanceRef();
}


/****************************************************************************
*
*   Transactions
*
***/

//===========================================================================
void DbBase::apply(uint32_t id, DbReq && req) {
    DbTxn txn{m_wal, m_page, m_data.metricRootsInstance()};
    switch (req.type) {
    case kGetMetric:
        m_data.getMetricInfo(req.notify, txn, id);
        break;
    case kGetSamples:
        m_data.getSamples(
            req.notify,
            txn,
            id,
            req.first,
            req.last,
            req.presamples
        );
        break;
    case kEraseMetric:
        if (m_data.eraseMetric(&req.name, txn, id)) {
            scoped_lock lk{m_indexMut};
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
            info.creation = req.first;
            m_data.updateMetric(txn, id, info);
        }
        break;
    case kUpdateSample:
        m_data.updateSample(txn, id, req.first, req.value);
        break;
    }

    auto freePages = txn.commit();
    m_data.publishFreePages(freePages);
}

//===========================================================================
bool DbBase::transact(uint32_t id, DbReq && req) {
    auto & bucket = m_reqBuckets[id % kRequestBuckets];
    unique_lock lk{bucket.mut};
    auto & reqs = bucket.requests[id];
    reqs.push_back(move(req));
    if (reqs.size() != 1)
        return false;

    while (!reqs.empty()) {
        req = move(reqs.front());
        lk.unlock();
        apply(id, move(req));
        lk.lock();
        reqs.pop_front();
    }
    bucket.requests.erase(id);
    return true;
}


/****************************************************************************
*
*   Metrics
*
***/

//===========================================================================
uint64_t DbBase::acquireInstanceRef() {
    scoped_lock lk{m_indexMut};
    return m_leaf.acquireInstanceRef();
}

//===========================================================================
void DbBase::releaseInstanceRef(uint64_t instance) {
    scoped_lock lk{m_indexMut};
    m_leaf.releaseInstanceRef(instance);
}

//===========================================================================
bool DbBase::insertMetric(uint32_t * out, string_view name) {
    {
        shared_lock lk{m_indexMut};
        if (m_leaf.find(out, name))
            return false;
    }

    {
        scoped_lock lk{m_indexMut};
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
    req.first = info.creation;
    transact(id, move(req));
}

//===========================================================================
const char * DbBase::getMetricName(uint32_t id) const {
    shared_lock lk{m_indexMut};
    return m_leaf.name(id);
}

//===========================================================================
bool DbBase::getMetricInfo(IDbDataNotify * notify, uint32_t id) const {
    auto self = const_cast<DbBase *>(this);
    DbReq req;
    req.type = kGetMetric;
    req.notify = notify;
    return self->transact(id, move(req));
}

//===========================================================================
bool DbBase::findMetric(uint32_t * out, string_view name) const {
    shared_lock lk{m_indexMut};
    return m_leaf.find(out, name);
}

//===========================================================================
void DbBase::findMetrics(UnsignedSet * out, string_view pattern) const {
    shared_lock lk{m_indexMut};
    m_leaf.find(out, pattern);
}

//===========================================================================
const char * DbBase::getBranchName(uint32_t id) const {
    shared_lock lk{m_indexMut};
    return m_branch.name(id);
}

//===========================================================================
void DbBase::findBranches(UnsignedSet * out, string_view pattern) const {
    shared_lock lk{m_indexMut};
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
bool DbBase::getSamples(
    IDbDataNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last,
    unsigned presamples
) {
    DbReq req;
    req.type = kGetSamples;
    req.notify = notify;
    req.first = first;
    req.last = last;
    req.presamples = presamples;
    return transact(id, move(req));
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
DbBase * dbOpen(
    string_view name,
    EnumFlags<DbOpenFlags> flags,
    size_t pageSize
) {
    auto db = make_unique<DbBase>();
    if (!db->open(name, flags, pageSize))
        return nullptr;

    return db.release();
}

//===========================================================================
void dbClose(DbBase * db) {
    if (db) {
        db->close();
        delete db;
    }
}

const TokenTable::Token s_sampleTypes[] = {
    { kSampleTypeFloat32,   "float32" },
    { kSampleTypeFloat64,   "float64" },
    { kSampleTypeInt8,      "int8" },
    { kSampleTypeInt16,     "int16" },
    { kSampleTypeInt32,     "int32" },
};
static_assert(size(s_sampleTypes) == kSampleTypes - 1);
const TokenTable s_sampleTypeTbl{s_sampleTypes};

//===========================================================================
const char * toString(DbSampleType type, const char def[]) {
    return s_sampleTypeTbl.findName(type, def);
}

//===========================================================================
DbSampleType fromString(std::string_view src, DbSampleType def) {
    return s_sampleTypeTbl.find(src, def);
}

//===========================================================================
void dbConfigure(DbBase * db, const DbConfig & conf) {
    db->configure(conf);
}

//===========================================================================
DbInfo dbQueryInfo(const DbBase * db) {
    return db->queryInfo();
}

//===========================================================================
DbStats dbQueryStats(const DbBase * db) {
    return db->queryStats();
}

//===========================================================================
void dbBlockCheckpoint(IDbProgressNotify * notify, DbBase * db, bool enable) {
    db->blockCheckpoint(notify, enable);
}

//===========================================================================
bool dbBackup(IDbProgressNotify * notify, DbBase * db, string_view dst) {
    return db->backup(notify, dst);
}

//===========================================================================
unique_ptr<DbContext> dbNewContext(DbBase * db) {
    auto ptr = make_unique<DbContext>(db);
    return ptr;
}

//===========================================================================
bool dbInsertMetric(uint32_t * out, DbBase * db, string_view name) {
    return db->insertMetric(out, name);
}

//===========================================================================
void dbEraseMetric(DbBase * db, uint32_t id) {
    db->eraseMetric(id);
}

//===========================================================================
void dbUpdateMetric(DbBase * db, uint32_t id, const DbMetricInfo & info) {
    db->updateMetric(id, info);
}

//===========================================================================
const char * dbGetMetricName(const DbBase * db, uint32_t id) {
    return db->getMetricName(id);
}

//===========================================================================
bool dbGetMetricInfo(IDbDataNotify * notify, const DbBase * db, uint32_t id) {
    return db->getMetricInfo(notify, id);
}

//===========================================================================
bool dbFindMetric(uint32_t * out, const DbBase * db, string_view name) {
    return db->findMetric(out, name);
}

//===========================================================================
void dbFindMetrics(UnsignedSet * out, const DbBase * db, string_view name) {
    db->findMetrics(out, name);
}

//===========================================================================
const char * dbGetBranchName(const DbBase * db, uint32_t id) {
    return db->getBranchName(id);
}

//===========================================================================
void dbFindBranches(UnsignedSet * out, const DbBase * db, string_view name) {
    db->findBranches(out, name);
}

//===========================================================================
void dbUpdateSample(
    DbBase * db,
    uint32_t id,
    TimePoint time,
    double value
) {
    db->updateSample(id, time, value);
}

//===========================================================================
bool dbGetSamples(
    IDbDataNotify * notify,
    DbBase * db,
    uint32_t id,
    TimePoint first,
    TimePoint last,
    unsigned presamples
) {
    return db->getSamples(notify, id, first, last, presamples);
}


/****************************************************************************
*
*   Public API - Types
*
***/

#if 0
//===========================================================================
std::strong_ordering Lsx::operator<=>(const Lsx & other) const {
    if (auto out = lsn <=> other.lsn; out != 0)
        return out;
    return localTxn <=> other.localTxn;
}
#endif

//===========================================================================
string toString(DbPageType type) {
    string out;
    auto val = to_underlying(type);
    out += (char) (val % 256);
    while (val /= 256)
        out += (char) (val % 256);
    return out;
}
