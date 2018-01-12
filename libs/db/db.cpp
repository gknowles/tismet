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

class DbBase : public HandleContent, public IDbEnumNotify {
public:
    DbBase();
    ~DbBase();

    bool open(string_view name, size_t pageSize, DbOpenFlags flags);
    void configure(const DbConfig & conf);
    DbStats queryStats();

    bool insertMetric(uint32_t & out, const string & name);
    void eraseMetric(uint32_t id);
    void updateMetric(
        uint32_t id,
        const MetricInfo & info
    );

    const char * getMetricName(uint32_t id) const;
    bool getMetricInfo(MetricInfo & info, uint32_t id) const;

    bool findMetric(uint32_t & out, const string & name) const;
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

    // Inherited via IDbEnumNotify
    void OnDbMetric(
        uint32_t id,
        string_view vname,
        DbSampleType type,
        TimePoint from,
        TimePoint until,
        Duration interval
    ) override;

private:
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
    : m_log(m_data, m_page)
{}

//===========================================================================
DbBase::~DbBase () {
}

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
    string_view vname,
    DbSampleType type,
    TimePoint from,
    TimePoint until,
    Duration interval
) {
    auto name = string(vname);
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
    DbStats s = m_data.queryStats();
    return s;
}


/****************************************************************************
*
*   Metrics
*
***/

//===========================================================================
bool DbBase::insertMetric(uint32_t & out, const string & name) {
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
void DbBase::updateMetric(
    uint32_t id,
    const MetricInfo & info
) {
    DbTxn txn{m_log, m_page};
    m_data.updateMetric(txn, id, info);
}

//===========================================================================
bool DbBase::findMetric(uint32_t & out, const string & name) const {
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

//===========================================================================
void dbConfigure(DbHandle h, const DbConfig & conf) {
    auto * db = s_files.find(h);
    assert(db);
    db->configure(conf);
}

//===========================================================================
DbStats dbQueryStats(DbHandle h) {
    auto * db = s_files.find(h);
    assert(db);
    return db->queryStats();
}

//===========================================================================
bool dbFindMetric(uint32_t & out, DbHandle h, string_view name) {
    auto * db = s_files.find(h);
    assert(db);
    return db->findMetric(out, string(name));
}

//===========================================================================
void dbFindMetrics(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view name
) {
    auto * db = s_files.find(h);
    assert(db);
    db->findMetrics(out, name);
}

//===========================================================================
const char * dbGetMetricName(DbHandle h, uint32_t id) {
    auto * db = s_files.find(h);
    assert(db);
    return db->getMetricName(id);
}

//===========================================================================
void dbFindBranches(
    Dim::UnsignedSet & out,
    DbHandle h,
    std::string_view name
) {
    auto * db = s_files.find(h);
    assert(db);
    db->findBranches(out, name);
}

//===========================================================================
const char * dbGetBranchName(DbHandle h, uint32_t id) {
    auto * db = s_files.find(h);
    assert(db);
    return db->getBranchName(id);
}

//===========================================================================
bool dbInsertMetric(uint32_t & out, DbHandle h, string_view name) {
    auto * db = s_files.find(h);
    assert(db);
    return db->insertMetric(out, string(name));
}

//===========================================================================
void dbEraseMetric(DbHandle h, uint32_t id) {
    auto * db = s_files.find(h);
    assert(db);
    db->eraseMetric(id);
}

//===========================================================================
bool dbGetMetricInfo(
    MetricInfo & info,
    DbHandle h,
    uint32_t id
) {
    auto * db = s_files.find(h);
    assert(db);
    return db->getMetricInfo(info, id);
}

//===========================================================================
void dbUpdateMetric(
    DbHandle h,
    uint32_t id,
    const MetricInfo & info
) {
    auto * db = s_files.find(h);
    assert(db);
    db->updateMetric(id, info);
}

//===========================================================================
void dbUpdateSample(
    DbHandle h,
    uint32_t id,
    TimePoint time,
    double value
) {
    auto * db = s_files.find(h);
    assert(db);
    db->updateSample(id, time, value);
}

//===========================================================================
size_t dbEnumSamples(
    IDbEnumNotify * notify,
    DbHandle h,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    auto * db = s_files.find(h);
    assert(db);
    return db->enumSamples(notify, id, first, last);
}
