// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dump.cpp - tismet data
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

const unsigned kMaxMetricNameLen = 64;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());


/****************************************************************************
*
*   Write dump
*
***/

namespace {

class DumpWriter : public ITsdEnumNotify {
public:
    DumpWriter(ostream & os);

    bool OnTsdValue(
        uint32_t id, 
        string_view name, 
        TimePoint time, 
        float val
    ) override;

private:
    ostream & m_os;
};

} // namespace

//===========================================================================
DumpWriter::DumpWriter(ostream & os) 
    : m_os{os}
{}

//===========================================================================
bool DumpWriter::OnTsdValue(
    uint32_t id, 
    string_view name, 
    TimePoint time, 
    float val
) {
    carbonWrite(m_os, name, time, val);
    return true;
}

//===========================================================================
// Public API
//===========================================================================
void tsdWriteDump(ostream & os, TsdFileHandle h, string_view wildname) {
    UnsignedSet ids;
    tsdFindMetrics(ids, h, wildname);
    os << kDumpVersion << '\n';
    DumpWriter out(os);
    for (auto && id : ids) {
        tsdEnumValues(&out, h, id);
    }
}


/****************************************************************************
*
*   Load dump
*
***/

namespace {

class TsdWriter 
    : public ICarbonNotify 
    , public IFileReadNotify
{
public:
    TsdWriter(ITsdProgressNotify * notify, TsdFileHandle h);

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
    TsdFileHandle m_tsd;
    ITsdProgressNotify * m_notify{nullptr};
    TsdProgressInfo m_info;
};

} // namespace

//===========================================================================
TsdWriter::TsdWriter(ITsdProgressNotify * notify, TsdFileHandle h)
    : m_notify{notify}
    , m_tsd{h}
{}

//===========================================================================
uint32_t TsdWriter::onCarbonMetric(string_view name) {
    uint32_t id;
    tsdInsertMetric(id, m_tsd, name);
    return id;
}

//===========================================================================
void TsdWriter::onCarbonValue(
    uint32_t id,
    TimePoint time,
    double value
) {
    m_info.values += 1;
    tsdUpdateValue(m_tsd, id, time, (float) value);
}

//===========================================================================
bool TsdWriter::onFileRead(
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
    if (!m_notify->OnTsdProgress(false, m_info))
        return false;
    return append(data);
}

//===========================================================================
void TsdWriter::onFileEnd(int64_t offset, FileHandle f) {
    m_info.totalMetrics = m_info.metrics;
    m_info.totalValues = m_info.values;
    if (m_info.totalBytes != (size_t) -1) 
        m_info.bytes = m_info.totalBytes;
    m_notify->OnTsdProgress(true, m_info);
    delete this;
}

//===========================================================================
// Public API
//===========================================================================
void tsdLoadDump(
    ITsdProgressNotify * notify,
    TsdFileHandle h,
    const Dim::Path & src
) {
    static const unsigned kBufferLen = 4096;
    // make sure there's room for the complete version info in the buffer
    static_assert(kBufferLen > size(kDumpVersion) + 2);
    auto writer = new TsdWriter{notify, h};
    fileStreamBinary(writer, src, kBufferLen);
}
