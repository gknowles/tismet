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

    bool open(string_view name, size_t pageSize);
    DbStats queryStats();

    bool insertMetric(uint32_t & out, const string & name);
    void eraseMetric(uint32_t id);
    bool getMetricInfo(MetricInfo & info, uint32_t id);
    void updateMetric(
        uint32_t id,
        const MetricInfo & info
    );

    bool findMetric(uint32_t & out, const string & name) const;
    void findMetrics(UnsignedSet & out, string_view name) const;

    void updateSample(uint32_t id, TimePoint time, float value);
    size_t enumSamples(
        IDbEnumNotify * notify,
        uint32_t id,
        TimePoint first,
        TimePoint last
    );

    // Inherited via IDbEnumNotify
    bool OnDbSample(
        uint32_t id,
        std::string_view name,
        Dim::TimePoint time,
        float value
    ) override;

private:
    void indexInsertMetric(
        uint32_t id,
        const string & name,
        bool inclByName = true
    );
    void indexEraseMetric(uint32_t id, const string & name);

    unordered_map<string, uint32_t> m_metricIds;
    UnsignedSet m_ids;

    struct UnsignedSetWithCount {
        UnsignedSet uset;
        size_t count = 0;
    };

    // metric ids by name length as measured in segments
    vector<UnsignedSetWithCount> m_lenIds;

    // Index of metric ids by the segments of their names. So the wildcard
    // *.red.* could be matched by finding all the metrics whose name has
    // "red" as the second segment (m_segIds[1]["red"]) and three segments
    // long (m_lenIds[3]).
    vector<unordered_map<string, UnsignedSetWithCount>> m_segIds;

    DbWork m_work;
    DbData m_data;
    DbLog m_log;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<DbHandle, DbBase> s_files;

static auto & s_perfCreated = uperf("metrics created");
static auto & s_perfDeleted = uperf("metrics deleted");


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
    : m_log(m_data, m_work)
{}

//===========================================================================
DbBase::~DbBase () {
}

//===========================================================================
bool DbBase::open(string_view name, size_t pageSize) {
    auto datafile = Path(name).setExt("tsd");
    auto workfile = Path(name).setExt("tsw");
    if (!m_work.open(datafile, workfile, pageSize))
        return false;
    DbTxn txn{m_log, m_work};
    return m_data.open(txn, this, datafile);
}

//===========================================================================
bool DbBase::OnDbSample(
    uint32_t id,
    std::string_view name,
    Dim::TimePoint time,
    float value
) {
    indexInsertMetric(id, string(name), true);
    return true;
}

//===========================================================================
DbStats DbBase::queryStats() {
    DbStats s = m_data.queryStats();
    s.metricIds = 0;
    for (auto & len : m_lenIds)
        s.metricIds += (unsigned) len.count;
    return s;
}


/****************************************************************************
*
*   Metrics
*
***/

//===========================================================================
bool DbBase::findMetric(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end())
        return false;
    out = i->second;
    return true;
}

//===========================================================================
void DbBase::findMetrics(UnsignedSet & out, string_view name) const {
    if (name.empty()) {
        out = m_ids;
        return;
    }

    QueryInfo qry;
    bool result [[maybe_unused]] = queryParse(qry, name);
    assert(result);
    if (~qry.flags & QueryInfo::fWild) {
        uint32_t id;
        out.clear();
        if (findMetric(id, string(name)))
            out.insert(id);
        return;
    }

    vector<QueryInfo::PathSegment> segs;
    queryPathSegments(segs, qry);
    auto numSegs = segs.size();
    vector<const UnsignedSetWithCount*> usets(numSegs);
    auto fewest = &m_lenIds[numSegs];
    int ifewest = -1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto & seg = segs[i];
        if (~seg.flags & QueryInfo::fWild) {
            auto it = m_segIds[i].find(string(seg.prefix));
            if (it != m_segIds[i].end()) {
                usets[i] = &it->second;
                if (it->second.count < fewest->count) {
                    ifewest = i;
                    fewest = &it->second;
                }
            }
        }
    }
    out = fewest->uset;
    for (int i = 0; i < numSegs; ++i) {
        if (i == ifewest)
            continue;
        if (auto usetw = usets[i]) {
            out.intersect(usetw->uset);
            continue;
        }
        auto & seg = segs[i];
        UnsignedSet found;
        for (auto && kv : m_segIds[i]) {
            if (queryMatchSegment(seg.node, kv.first)) {
                if (found.empty()) {
                    found = kv.second.uset;
                } else {
                    found.insert(kv.second.uset);
                }
            }
        }
        out.intersect(move(found));
    }
}

//===========================================================================
void DbBase::indexInsertMetric(
    uint32_t id,
    const string & name,
    bool inclByName
) {
    if (inclByName) {
        if (!m_metricIds.insert({name, id}).second)
            logMsgError() << "Metric multiply defined, " << name;
    }
    m_ids.insert(id);
    vector<string_view> segs;
    strSplit(segs, name, '.');
    auto numSegs = segs.size();
    if (m_lenIds.size() <= numSegs) {
        m_lenIds.resize(numSegs + 1);
        m_segIds.resize(numSegs);
    }
    m_lenIds[numSegs].uset.insert(id);
    m_lenIds[numSegs].count += 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto & ids = m_segIds[i][string(segs[i])];
        ids.uset.insert(id);
        ids.count += 1;
    }
}

//===========================================================================
void DbBase::indexEraseMetric(uint32_t id, const string & name) {
    auto num [[maybe_unused]] = m_metricIds.erase(name);
    assert(num == 1);
    m_ids.erase(id);
    vector<string_view> segs;
    strSplit(segs, name, '.');
    auto numSegs = segs.size();
    m_lenIds[numSegs].uset.erase(id);
    m_lenIds[numSegs].count -= 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto key = string(segs[i]);
        auto & ids = m_segIds[i][key];
        ids.uset.erase(id);
        if (--ids.count == 0)
            m_segIds[i].erase(key);
    }
    numSegs = m_segIds.size();
    for (; numSegs; --numSegs) {
        if (!m_segIds[numSegs - 1].empty())
            break;
        assert(m_lenIds[numSegs].uset.empty());
        m_lenIds.resize(numSegs);
        m_segIds.resize(numSegs - 1);
    }
}

//===========================================================================
bool DbBase::insertMetric(uint32_t & out, const string & name) {
    auto i = m_metricIds.find(name);
    if (i != m_metricIds.end()) {
        out = i->second;
        return false;
    }

    // get metric id
    uint32_t id;
    if (m_ids.empty()) {
        id = 1;
    } else {
        auto ids = *m_ids.ranges().begin();
        id = ids.first > 1 ? 1 : ids.second + 1;
    }
    out = id;

    // update indexes
    indexInsertMetric(id, name);

    // set info page
    DbTxn txn{m_log, m_work};
    m_data.insertMetric(txn, id, name);
    s_perfCreated += 1;
    return true;
}

//===========================================================================
void DbBase::eraseMetric(uint32_t id) {
    DbTxn txn{m_log, m_work};
    string name;
    if (m_data.eraseMetric(txn, name, id)) {
        indexEraseMetric(id, name);
        s_perfDeleted += 1;
    }
}

//===========================================================================
bool DbBase::getMetricInfo(MetricInfo & info, uint32_t id) {
    DbTxn txn{m_log, m_work};
    return m_data.getMetricInfo(txn, info, id);
}

//===========================================================================
void DbBase::updateMetric(
    uint32_t id,
    const MetricInfo & info
) {
    DbTxn txn{m_log, m_work};
    m_data.updateMetric(txn, id, info);
}


/****************************************************************************
*
*   Samples
*
***/

//===========================================================================
void DbBase::updateSample(uint32_t id, TimePoint time, float value) {
    DbTxn txn{m_log, m_work};
    m_data.updateSample(txn, id, time, value);
}

//===========================================================================
size_t DbBase::enumSamples(
    IDbEnumNotify * notify,
    uint32_t id,
    TimePoint first,
    TimePoint last
) {
    DbTxn txn{m_log, m_work};
    return m_data.enumSamples(txn, notify, id, first, last);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
DbHandle dbOpen(string_view name, size_t pageSize) {
    auto db = make_unique<DbBase>();
    if (!db->open(name, pageSize))
        return DbHandle{};

    auto h = s_files.insert(db.release());
    return h;
}

//===========================================================================
void dbClose(DbHandle h) {
    s_files.erase(h);
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
    float value
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
