// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dbindex.h - tismet db
#pragma once

#include "core/core.h"
#include "query/query.h"

#include <cstdint>
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
        size_t count = 0;
    };

public:
    void clear();

    void insert(uint32_t id, std::string_view name, bool branch = false);
    void erase(uint32_t id, std::string_view name, bool branch = false);

    // A branch is the string consisting of one or more segments prefixing
    // the name of a metric name. A string is both a branch and a metric if
    // there are additional metrics for which it is a prefix.
    void insertBranches(uint32_t id, std::string_view name);
    void eraseBranches(uint32_t id, std::string_view name);

    uint32_t nextId() const;
    size_t size() const;

    const char * name(uint32_t id) const;

    bool find(uint32_t & out, const std::string & name) const;
    void find(Dim::UnsignedSet & out, std::string_view name) const;

private:
    void find(
        Dim::UnsignedSet & out,
        QueryInfo::PathSegment * segs,
        size_t numSegs,
        size_t pos,
        const UnsignedSetWithCount * subset
    ) const;

    std::vector<const char *> m_idNames;
    std::unordered_map<std::string, std::pair<uint32_t, unsigned>> m_metricIds;
    UnsignedSetWithCount m_ids;

    // metric ids by name length as measured in segments
    std::vector<UnsignedSetWithCount> m_lenIds;

    // Index of metric ids by the segments of their names. So the wildcard
    // *.red.* could be matched by finding all the metrics whose name has
    // "red" as the second segment (m_segIds[1]["red"]) and is three segments
    // long (m_lenIds[3]).
    std::vector<std::unordered_map<std::string, UnsignedSetWithCount>> m_segIds;
};
