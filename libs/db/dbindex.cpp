// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbindex.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;
using namespace Query;


/****************************************************************************
*
*   DbIndex
*
***/

//===========================================================================
DbIndex::~DbIndex() {
    clear();
}

//===========================================================================
void DbIndex::clear() {
    m_nextBranchId = 0;
    m_branchErasures = false;
    m_idNames.clear();
    m_metricIds.clear();
    m_ids.uset.clear();
    m_ids.count = 0;

    m_unusedIds.clear();
    m_instance = 0;
    m_reservedIds.clear();

    m_lenIds.clear();
    m_segIds.clear();
    for (auto && sn : m_segNames) {
        for (auto && kv : sn) {
            delete[] kv.second;
        }
    }
    m_segNames.clear();
    m_tmpSegs.clear();
}

//===========================================================================
uint64_t DbIndex::acquireInstanceRef() {
    auto & info = m_reservedIds[m_instance];
    info.refCount += 1;
    return m_instance;
}

//===========================================================================
void DbIndex::releaseInstanceRef(uint64_t instance) {
    auto & info = m_reservedIds[m_instance];
    if (!--info.refCount) {
        auto i = m_reservedIds.begin();
        while (i != m_reservedIds.end()) {
            if (i->second.refCount)
                return;
            m_unusedIds.insert(move(i->second.ids));
            i = m_reservedIds.erase(i);
        }
    }
}

//===========================================================================
void DbIndex::insertBranches(string_view name) {
    for (;;) {
        auto pos = name.find_last_of('.');
        if (pos == string_view::npos)
            return;
        name.remove_suffix(name.size() - pos);
        auto id = m_branchErasures ? nextId() : ++m_nextBranchId;
        if (auto i = m_metricIds.find(name); i != m_metricIds.end()) {
            i->second.second += 1;
        } else {
            insert(id, name);
        }
    }
}

//===========================================================================
void DbIndex::insert(uint32_t id, string_view name) {
    auto ptr = strDup(name);
    name = string_view{ptr.get(), name.size()};
    auto ib = m_metricIds.insert({name, {id, 1}});
    if (!ib.second) {
        logMsgCrash() << "Metric multiply defined, " << name;
        return;
    }

    if (id >= m_idNames.size())
        m_idNames.resize(id + 1);
    m_idNames[id] = move(ptr);

    m_ids.uset.insert(id);
    m_ids.count += 1;
    m_unusedIds.erase(id);
    strSplit(&m_tmpSegs, name, '.');
    auto numSegs = m_tmpSegs.size();
    if (m_lenIds.size() <= numSegs) {
        m_lenIds.resize(numSegs + 1);
        m_segIds.resize(numSegs);
        m_segNames.resize(numSegs);
    }
    m_lenIds[numSegs].uset.insert(id);
    m_lenIds[numSegs].count += 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto seg = m_tmpSegs[i];
        auto cur = m_segIds[i].find(seg);
        if (cur == m_segIds[i].end()) {
            auto ptr = strDup(seg).release();
            seg = string_view{ptr, seg.size()};
            m_segNames[i][seg] = ptr;
            cur = m_segIds[i].insert({seg, {}}).first;
        }
        auto & ids = cur->second;
        ids.uset.insert(id);
        ids.count += 1;
    }
}

//===========================================================================
void DbIndex::eraseBranches(string_view name) {
    m_branchErasures = true;
    for (;;) {
        auto pos = name.find_last_of('.');
        if (pos == string_view::npos)
            return;
        name.remove_suffix(name.size() - pos);
        erase(name);
    }
}

//===========================================================================
void DbIndex::erase(string_view name) {
    auto i = m_metricIds.find(name);
    if (--i->second.second)
        return;
    auto id = i->second.first;
    m_metricIds.erase(i);
    m_idNames[id] = nullptr;

    m_instance += 1;
    m_ids.uset.erase(id);
    m_ids.count -= 1;
    if (m_reservedIds.empty()) {
        m_unusedIds.insert(id);
    } else {
        m_reservedIds.rbegin()->second.ids.insert(id);
    }

    vector<string_view> segs;
    strSplit(&segs, name, '.');
    auto numSegs = segs.size();
    m_lenIds[numSegs].uset.erase(id);
    m_lenIds[numSegs].count -= 1;
    for (unsigned i = 0; i < numSegs; ++i) {
        auto & key = segs[i];
        auto & ids = m_segIds[i][key];
        ids.uset.erase(id);
        if (--ids.count == 0) {
            m_segIds[i].erase(key);
            auto cur = m_segNames[i].find(key);
            delete[] cur->second;
            m_segNames[i].erase(cur);
        }
    }
    numSegs = m_segIds.size();
    for (; numSegs; --numSegs) {
        if (!m_segIds[numSegs - 1].empty())
            break;
        assert(m_lenIds[numSegs].uset.empty());
        m_lenIds.resize(numSegs);
        m_segIds.resize(numSegs - 1);
        m_segNames.resize(numSegs - 1);
    }
}

//===========================================================================
uint32_t DbIndex::nextId() const {
    if (!m_unusedIds.empty()) {
        return *m_unusedIds.begin();
    } else if (!m_ids.count) {
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
    return id < m_idNames.size() ? m_idNames[id].get() : nullptr;
}

//===========================================================================
bool DbIndex::find(uint32_t * out, string_view name) const {
    auto i = m_metricIds.find(name);
    if (i == m_metricIds.end()) {
        *out = 0;
        return false;
    }
    *out = i->second.first;
    return true;
}

//===========================================================================
void DbIndex::find(
    UnsignedSet * out,
    PathSegment * segs,
    size_t numSegs,
    size_t basePos,
    const UnsignedSetWithCount * subset
) const {
    assert(basePos + numSegs < m_lenIds.size());
    out->clear();
    if (!numSegs || subset && subset->count == 0)
        return;

    vector<const UnsignedSetWithCount*> usets(numSegs);
    const UnsignedSetWithCount * fewest = subset;
    int ifewest = -1;
    int pos = (int) basePos;
    for (int i = 0; i < numSegs; ++i) {
        auto & seg = segs[i];
        if (seg.type == kExact) {
            assert(pos + i < m_segIds.size());
            auto it = m_segIds[pos + i].find(seg.prefix);
            if (it == m_segIds[pos + i].end())
                return;
            usets[i] = &it->second;
            if (!fewest || it->second.count < fewest->count) {
                ifewest = i;
                fewest = &it->second;
            }
        } else if (seg.type == kDynamicAny) {
            pos += seg.count - 1;
        }
    }
    if (fewest) {
        *out = fewest->uset;
        if (subset && fewest != subset) {
            out->intersect(subset->uset);
            if (out->empty())
                return;
        }
    }
    pos = (int) basePos;
    for (int i = 0; i < numSegs; ++i) {
        if (i == ifewest)
            continue;
        if (auto usetw = usets[i]) {
            if (out->empty()) {
                *out = usetw->uset;
            } else {
                out->intersect(usetw->uset);
                if (out->empty())
                    return;
            }
            continue;
        }
        auto & seg = segs[i];
        if (seg.type == kAny)
            continue;
        if (seg.type == kDynamicAny) {
            pos += seg.count - 1;
            continue;
        }
        assert(seg.type == kCondition);
        UnsignedSet found;
        auto & sids = m_segIds[pos + i];
        auto it = sids.lower_bound(seg.prefix);
        for (; it != sids.end(); ++it) {
            auto & [k, v] = *it;
            auto vk = string_view{k}.substr(0, seg.prefix.size());
            if (vk != seg.prefix)
                break;
            if (matchSegment(*seg.node, k))
                found.insert(v.uset);
        }
        if (out->empty()) {
            *out = move(found);
        } else {
            out->intersect(move(found));
        }
        if (out->empty())
            return;
    }
}

//===========================================================================
void DbIndex::find(UnsignedSet * out, string_view name) const {
    if (name.empty()) {
        *out = m_ids.uset;
        return;
    }

    QueryInfo qry;
    if (!parse(qry, name)) {
        out->clear();
        return;
    }
    if (qry.type == kExact) {
        uint32_t id;
        out->clear();
        if (find(&id, name))
            out->insert(id);
        return;
    }
    if (qry.type == kAny) {
        *out = m_ids.uset;
        return;
    }

    vector<PathSegment> segs;
    getPathSegments(&segs, qry);
    auto numSegs = segs.size();
    vector<unsigned> dyns;
    unsigned numStatic = 0;
    for (unsigned i = 0; i < numSegs; ++i) {
        if (segs[i].type == kDynamicAny) {
            dyns.push_back(i);
        } else {
            numStatic += 1;
        }
    }
    if (numStatic >= m_lenIds.size()) {
        // query requires more segments than any metric has
        out->clear();
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
    out->swap(prefixIds.uset);
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
        find(&found, segbase, seglen, prefix, ssptr);
        if (prefix)
            found.intersect(ssptr == &lens ? prefixIds.uset : lens.uset);
        out->insert(move(found));

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
