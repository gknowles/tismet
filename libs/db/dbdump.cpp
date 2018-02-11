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

class DumpWriter : public IDbDataNotify {
public:
    explicit DumpWriter(ostream & os, DbProgressInfo & info);

    bool onDbSeriesStart(const DbSeriesInfo & info) override;
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
bool DumpWriter::onDbSeriesStart(const DbSeriesInfo & info) {
    m_name = info.name;
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
    DbHandle f,
    string_view wildname
) {
    UnsignedSet ids;
    auto ctx = dbOpenContext(f);
    dbFindMetrics(&ids, f, wildname);
    os << kDumpVersion << '\n';
    DbProgressInfo info;
    DumpWriter out(os, info);
    for (auto && id : ids) {
        dbGetSamples(&out, f, id);
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
    dbCloseContext(ctx);
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
    ~DbWriter();

    // Inherited via ICarbonNotify
    void onCarbonValue(
        string_view name,
        TimePoint time,
        double value,
        uint32_t hintId
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
    IDbProgressNotify * m_notify{nullptr};
    DbHandle m_f;
    DbContextHandle m_cxt;
    DbProgressInfo m_info;
};

} // namespace

//===========================================================================
DbWriter::DbWriter(IDbProgressNotify * notify, DbHandle f)
    : m_notify{notify}
    , m_f{f}
{
    m_cxt = dbOpenContext(m_f);
}

//===========================================================================
DbWriter::~DbWriter() {
    dbCloseContext(m_cxt);
}

//===========================================================================
void DbWriter::onCarbonValue(
    string_view name,
    TimePoint time,
    double value,
    uint32_t id
) {
    m_info.samples += 1;
    dbInsertMetric(&id, m_f, name);
    dbUpdateSample(m_f, id, time, (float) value);
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
