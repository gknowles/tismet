// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbindex.h - tismet db
#pragma once

#include "core/core.h"
#include "query/query.h"

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>


/****************************************************************************
*
*   DbIndex
*
***/

class DbIndex {
public:
    struct UnsignedSetWithCount {
        Dim::UnsignedSet uset;
        size_t count{0};
    };

public:
    ~DbIndex();

    void clear();
    uint64_t acquireInstanceRef();
    void releaseInstanceRef(uint64_t instance);

    void insert(uint32_t id, std::string_view name, bool branch = false);
    void erase(std::string_view name);

    // A branch is the string consisting of one or more segments prefixing
    // the name of a metric name. A string is both a branch and a metric if
    // there are additional metrics for which it is a prefix.
    void insertBranches(std::string_view name);
    void eraseBranches(std::string_view name);

    uint32_t nextId() const;
    size_t size() const;

    const char * name(uint32_t id) const;

    bool find(uint32_t * out, std::string_view name) const;
    void find(Dim::UnsignedSet * out, std::string_view name) const;

private:
    void find(
        Dim::UnsignedSet * out,
        Query::PathSegment * segs,
        size_t numSegs,
        size_t pos,
        const UnsignedSetWithCount * subset
    ) const;

    uint32_t m_nextBranchId{0};
    bool m_branchErasures{false};
    std::vector<std::unique_ptr<char[]>> m_idNames;
    std::unordered_map<std::string_view, std::pair<uint32_t, unsigned>>
        m_metricIds;
    UnsignedSetWithCount m_ids;

    Dim::UnsignedSet m_unusedIds;
    uint64_t m_instance;
    struct InstanceInfo {
        int refCount{0};
        Dim::UnsignedSet ids;
    };
    std::map<uint64_t, InstanceInfo> m_reservedIds;

    // metric ids by name length as measured in segments
    std::vector<UnsignedSetWithCount> m_lenIds;

    // Index of metric ids by the segments of their names. So the wildcard
    // *.red.* could be matched by finding all the metrics whose name has
    // "red" as the second segment (m_segIds[1]["red"]) and is three segments
    // long (m_lenIds[3]).
    std::vector<std::unordered_map<std::string_view, const char*>> m_segNames;
    std::vector<std::map<std::string_view, UnsignedSetWithCount>> m_segIds;

    std::vector<std::string_view> m_tmpSegs;
};
