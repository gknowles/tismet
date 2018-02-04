// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// dbdump.cpp - tismet db
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

const char kDumpVersion[] = "Tismet Dump Version 2017.1";


/****************************************************************************
*
*   Write dump
*
***/

namespace {

class DumpWriter : public IDbEnumNotify {
public:
    explicit DumpWriter(ostream & os, DbProgressInfo & info);

    bool onDbSeriesStart(
        string_view query,
        uint32_t id,
        string_view name,
        DbSampleType type,
        TimePoint from,
        TimePoint until,
        Duration interval
    ) override;
    bool onDbSample(TimePoint time, double val) override;

private:
    string_view m_name;
    ostream & m_os;
    DbProgressInfo & m_info;
    string m_buf;
};

} // namespace

//===========================================================================
DumpWriter::DumpWriter(ostream & os, DbProgressInfo & info)
    : m_os{os}
    , m_info{info}
{}

//===========================================================================
bool DumpWriter::onDbSeriesStart(
    string_view query,
    uint32_t id,
    string_view name,
    DbSampleType type,
    TimePoint from,
    TimePoint until,
    Duration interval
) {
    m_name = name;
    return true;
}

//===========================================================================
bool DumpWriter::onDbSample(TimePoint time, double val) {
    m_buf.clear();
    carbonWrite(m_buf, m_name, time, val);
    m_os << m_buf;
    m_info.bytes += m_buf.size();
    m_info.samples += 1;
    return true;
}

//===========================================================================
// Public API
//===========================================================================
void dbWriteDump(
    IDbProgressNotify * notify,
    ostream & os,
    DbHandle h,
    string_view wildname
) {
    UnsignedSet ids;
    dbFindMetrics(ids, h, wildname);
    os << kDumpVersion << '\n';
    DbProgressInfo info;
    DumpWriter out(os, info);
    for (auto && id : ids) {
        dbEnumSamples(&out, h, id);
        info.metrics += 1;
        if (notify)
            notify->onDbProgress(kRunRunning, info);
    }
    info.totalMetrics = info.metrics;
    info.totalSamples = info.samples;
    if (info.totalBytes != (size_t) -1)
        info.bytes = info.totalBytes;
    if (notify)
        notify->onDbProgress(kRunStopped, info);
}


/****************************************************************************
*
*   Load dump
*
***/

namespace {

class DbWriter
    : public ICarbonNotify
    , public IFileReadNotify
{
public:
    DbWriter(IDbProgressNotify * notify, DbHandle h);

    // Inherited via ICarbonNotify
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(
        uint32_t id,
        TimePoint time,
        double value
    ) override;

    // Inherited via IFileReadNotify
    bool onFileRead(
        size_t * bytesUsed,
        string_view data,
        int64_t offset,
        FileHandle f
    ) override;
    void onFileEnd(int64_t offset, FileHandle f) override;

private:
    DbHandle m_db;
    IDbProgressNotify * m_notify{nullptr};
    DbProgressInfo m_info;
};

} // namespace

//===========================================================================
DbWriter::DbWriter(IDbProgressNotify * notify, DbHandle h)
    : m_db{h}
    , m_notify{notify}
{}

//===========================================================================
uint32_t DbWriter::onCarbonMetric(string_view name) {
    uint32_t id;
    dbInsertMetric(id, m_db, name);
    return id;
}

//===========================================================================
void DbWriter::onCarbonValue(
    uint32_t id,
    TimePoint time,
    double value
) {
    m_info.samples += 1;
    dbUpdateSample(m_db, id, time, (float) value);
}

//===========================================================================
bool DbWriter::onFileRead(
    size_t * bytesUsed,
    string_view data,
    int64_t offset,
    FileHandle f
) {
    *bytesUsed = data.size();
    m_info.bytes = offset;
    if (!offset) {
        m_info.totalBytes = fileSize(f);

        // check dump version
        if (data.substr(0, size(kDumpVersion) - 1) != kDumpVersion) {
            logMsgError() << filePath(f) << ": Unknown dump format";
            return false;
        }
        data.remove_prefix(size(kDumpVersion));
        while (!data.empty() && (data[0] == '\r' || data[0] == '\n'))
            data.remove_prefix(1);
    }
    if (!m_notify->onDbProgress(kRunRunning, m_info))
        return false;
    return append(data);
}

//===========================================================================
void DbWriter::onFileEnd(int64_t offset, FileHandle f) {
    m_info.totalMetrics = m_info.metrics;
    m_info.totalSamples = m_info.samples;
    if (m_info.totalBytes != (size_t) -1)
        m_info.bytes = m_info.totalBytes;
    m_notify->onDbProgress(kRunStopped, m_info);
    delete this;
}

//===========================================================================
// Public API
//===========================================================================
void dbLoadDump(
    IDbProgressNotify * notify,
    DbHandle h,
    const Dim::Path & src
) {
    static const unsigned kBufferLen = 4096;
    // make sure there's room for the complete version info in the buffer
    static_assert(kBufferLen > size(kDumpVersion) + 2);
    auto writer = new DbWriter{notify, h};
    fileStreamBinary(writer, src, kBufferLen);
}
