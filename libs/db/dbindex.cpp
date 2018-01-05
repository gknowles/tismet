// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbindex.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   DbIndex
*
***/

//===========================================================================
void DbIndex::clear() {
    m_metricIds.clear();
    m_ids.uset.clear();
    m_ids.count = 0;
    m_lenIds.clear();
    m_segIds.clear();
}

//===========================================================================
void DbIndex::insert(uint32_t id, const string & name) {
    auto ib = m_metricIds.insert({name, id});
    if (!ib.second)
        logMsgError() << "Metric multiply defined, " << name;
    if (id >= m_idNames.size())
        m_idNames.resize(id + 1);
    m_idNames[id] = ib.first->first.c_str();
    m_ids.uset.insert(id);
    m_ids.count += 1;
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
void DbIndex::erase(uint32_t id, const string & name) {
    auto num [[maybe_unused]] = m_metricIds.erase(name);
    assert(num == 1);
    m_idNames[id] = nullptr;
    m_ids.uset.erase(id);
    m_ids.count -= 1;
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
uint32_t DbIndex::nextId() const {
    if (!m_ids.count) {
        return 1;
    } else {
        auto ids = *m_ids.uset.ranges().begin();
        return ids.first > 1 ? 1 : ids.second + 1;
    }
}

//===========================================================================
size_t DbIndex::size() const {
    return m_ids.count;
}

//===========================================================================
const char * DbIndex::name(uint32_t id) const {
    return id < m_idNames.size() ? m_idNames[id] : nullptr;
}

//===========================================================================
bool DbIndex::find(uint32_t & out, const string & name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end()) {
        out = 0;
        return false;
    }
    out = i->second;
    return true;
}

//===========================================================================
void DbIndex::find(
    UnsignedSet & out,
    QueryInfo::PathSegment * segs,
    size_t numSegs,
    size_t basePos,
    const UnsignedSetWithCount * subset
) const {
    assert(basePos + numSegs < m_lenIds.size());
    if (!numSegs) {
        out.clear();
        return;
    }
    vector<const UnsignedSetWithCount*> usets(numSegs);
    const UnsignedSetWithCount * fewest = subset;
    int ifewest = -1;
    int pos = (int) basePos;
    for (int i = 0; i < numSegs; ++i) {
        auto & seg = segs[i];
        if (seg.type == QueryInfo::kExact) {
            assert(pos + i < m_segIds.size());
            auto it = m_segIds[pos + i].find(string(seg.prefix));
            if (it == m_segIds[pos + i].end()) {
                out.clear();
                return;
            }
            usets[i] = &it->second;
            if (!fewest || it->second.count < fewest->count) {
                ifewest = i;
                fewest = &it->second;
            }
        } else if (seg.type == QueryInfo::kDynamicAny) {
            pos += seg.count - 1;
        }
    }
    if (fewest) {
        out = fewest->uset;
        if (subset && fewest != subset)
            out.intersect(subset->uset);
    } else {
        out.clear();
    }
    pos = (int) basePos;
    for (int i = 0; i < numSegs; ++i) {
        if (i == ifewest)
            continue;
        if (auto usetw = usets[i]) {
            if (out.empty()) {
                out = usetw->uset;
            } else {
                out.intersect(usetw->uset);
            }
            if (out.empty())
                return;
            continue;
        }
        auto & seg = segs[i];
        if (seg.type == QueryInfo::kAny)
            continue;
        if (seg.type == QueryInfo::kDynamicAny) {
            pos += seg.count - 1;
            continue;
        }
        assert(seg.type == QueryInfo::kCondition);
        UnsignedSet found;
        for (auto && kv : m_segIds[pos + i]) {
            if (queryMatchSegment(seg.node, kv.first)) {
                if (found.empty()) {
                    found = kv.second.uset;
                } else {
                    found.insert(kv.second.uset);
                }
            }
        }
        if (out.empty()) {
            out = move(found);
        } else {
            out.intersect(move(found));
        }
        if (out.empty())
            return;
    }
}

//===========================================================================
void DbIndex::find(UnsignedSet & out, string_view name) const {
    if (name.empty()) {
        out = m_ids.uset;
        return;
    }

    QueryInfo qry;
    if (!queryParse(qry, name)) {
        out.clear();
        return;
    }
    if (qry.type == QueryInfo::kExact) {
        uint32_t id;
        out.clear();
        if (find(id, string(name)))
            out.insert(id);
        return;
    }
    if (qry.type == QueryInfo::kAny) {
        out = m_ids.uset;
        return;
    }

    vector<QueryInfo::PathSegment> segs;
    queryPathSegments(segs, qry);
    auto numSegs = segs.size();
    vector<unsigned> dyns;
    unsigned numStatic = 0;
    for (unsigned i = 0; i < numSegs; ++i) {
        if (segs[i].type == QueryInfo::kDynamicAny) {
            dyns.push_back(i);
        } else {
            numStatic += 1;
        }
    }
    if (numStatic >= m_lenIds.size()) {
        // query requires more segments than any metric has
        out.clear();
        return;
    }

    if (dyns.empty()) {
        // The subset is the set of metrics that would match if all path
        // segments match any. So if the query is completely static the subset
        // is metrics with that number of segments.
        return find(
            out,
            segs.data(),
            numSegs,
            0,
            &m_lenIds[numSegs]
        );
    }

    // There are dynamic segments, so the subset is all metrics with at least
    // that number of segments, since metrics match when the initial segments
    // match the prefix.
    //
    // For now, we don't prefilter in the dynamic case.
    auto prefix = dyns[0];
    find(out, segs.data(), prefix, 0, nullptr);

    // If all statics are clustered at the front there's no need to try any
    // other permutations.
    if (numStatic == prefix)
        return;

    UnsignedSetWithCount prefixIds;
    out.swap(prefixIds.uset);
    prefixIds.count = prefixIds.uset.size();
    auto segbase = segs.data() + prefix;
    auto seglen = segs.size() - prefix;
    unsigned numDyn = 0;
    unsigned maxDyn = (unsigned) m_lenIds.size() - numStatic - 1;
    UnsignedSetWithCount subsetw;
    for (;;) {
        auto & lens = m_lenIds[numStatic + numDyn];
        auto ssptr = &lens;
        if (prefix && prefixIds.count < lens.count)
            ssptr = &prefixIds;

        UnsignedSet found;
        find(found, segbase, seglen, prefix, ssptr);
        if (prefix)
            found.intersect(ssptr == &lens ? prefixIds.uset : lens.uset);
        out.insert(move(found));

        for (unsigned i = 0;;) {
            auto & seg = segs[dyns[i]];
            if (numDyn < maxDyn) {
                seg.count += 1;
                numDyn += 1;
                break;
            }
            numDyn -= seg.count;
            seg.count = 0;
            if (++i == dyns.size())
                return;
        }
    }
}
