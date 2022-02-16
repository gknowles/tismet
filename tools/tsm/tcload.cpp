// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// tcload.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CmdOpts {
    Path database;
    Path dumpfile;
    bool truncate{false};

    CmdOpts();
};

class DumpReader
    : public IFileReadNotify
    , MsgPack::IParserNotify
{
public:
    DumpReader();

    virtual bool onDumpMetrics(size_t totalMetrics) = 0;
    virtual bool onDumpSeries(const DbSeriesInfoEx & ex) = 0;
    virtual bool onDumpSample(double value) = 0;
    virtual void onDumpEnd() = 0;

private:
    // Inherited via IFileReadNotify
    bool onFileRead(size_t * bytesUsed, const FileReadData & data) override;

    // Inherited via IParserNotify
    bool startArray(size_t length) override;
    bool startMap(size_t length) override;
    bool valuePrefix(std::string_view val, bool first) override;
    bool value(std::string_view val) override;
    bool value(double val) override;
    bool value(int64_t val) override;
    bool value(uint64_t val) override;
    bool value(bool val) override;
    bool value(std::nullptr_t) override;

    bool nextMetric();

    MsgPack::StreamParser m_parser;

    enum class State {
        kStartFileArray,
        kStartMetaMap,
        kVersionKey,
        kVersionValue,
        kStartMetricsArray,
        kStartMetricArray,
        kMetricName,
        kMetricType,
        kMetricCreation,
        kMetricRetention,
        kMetricInterval,
        kMetricFirstTime,
        kStartSamplesArray,
        kSample,
        kDone,
    };
    State m_state{State::kStartFileArray};
    size_t m_metrics{0};
    size_t m_samples{0};
    string m_tmp;
    string m_name;
    DbSeriesInfoEx m_ex;
};

class DbWriter : public DumpReader {
public:
    bool onDumpMetrics(size_t totalMetrics) override;
    bool onDumpSeries(const DbSeriesInfoEx & ex) override;
    bool onDumpSample(double value) override;
    void onDumpEnd() override;

private:
    uint32_t m_id{0};
    TimePoint m_time;
    Duration m_interval;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static DbProgressInfo s_progress;
static DbHandle s_db;
static DbWriter s_writer;


/****************************************************************************
*
*   DumpReader
*
***/

//===========================================================================
DumpReader::DumpReader()
    : m_parser{this}
{}

//===========================================================================
bool DumpReader::onFileRead(
    size_t * bytesUsed,
    const FileReadData & data
) {
    auto more = data.more;
    auto ec = m_parser.parse(bytesUsed, data.data);
    s_progress.bytes += *bytesUsed;
    more = more && (!ec || ec == errc::operation_in_progress);
    if (!more)
        onDumpEnd();
    return more;
}

//===========================================================================
bool DumpReader::startArray(size_t length) {
    switch (m_state) {
    case State::kStartFileArray:
        m_state = State::kStartMetaMap;
        return length == 2;
    case State::kStartMetricsArray:
        if (!onDumpMetrics(length))
            return false;
        if (length) {
            m_metrics = length;
            m_state = State::kStartMetricArray;
            s_progress.totalMetrics = length;
        } else {
            m_state = State::kDone;
        }
        return true;
    case State::kStartMetricArray:
        m_state = State::kMetricName;
        return length == 7;
    case State::kStartSamplesArray:
        m_samples = length;
        if (!length)
            return nextMetric();
        m_state = State::kSample;
        s_progress.totalSamples += length;
        return true;
    default:
        return false;
    }
}

//===========================================================================
bool DumpReader::startMap(size_t length) {
    switch (m_state) {
    case State::kStartMetaMap:
        m_state = State::kVersionKey;
        return length == 1;
    default:
        return false;
    }
}

//===========================================================================
bool DumpReader::valuePrefix(std::string_view val, bool first) {
    m_tmp.append(val);
    return true;
}

//===========================================================================
bool DumpReader::value(std::string_view val) {
    string tmp;
    if (!m_tmp.empty()) {
        tmp.swap(m_tmp);
        tmp.append(val);
        val = tmp;
    }
    switch (m_state) {
    case State::kVersionKey:
        if (val != "Tismet Dump Version")
            return false;
        m_state = State::kVersionValue;
        return true;
    case State::kVersionValue:
        if (val != "2018.1")
            return false;
        m_state = State::kStartMetricsArray;
        return true;
    case State::kMetricName:
        m_name = val;
        m_ex.name = m_name;
        m_state = State::kMetricType;
        return true;
    case State::kMetricType:
        m_ex.type = fromString(val, kSampleTypeInvalid);
        m_state = State::kMetricCreation;
        return m_ex.type != kSampleTypeInvalid;
    default:
        return false;
    }
}

//===========================================================================
bool DumpReader::value(bool val) {
    return false;
}

//===========================================================================
bool DumpReader::value(std::nullptr_t) {
    return value(NAN);
}

//===========================================================================
bool DumpReader::value(double val) {
    switch (m_state) {
    case State::kSample:
        s_progress.samples += 1;
        if (!onDumpSample(val))
            return false;
        if (!--m_samples)
            return nextMetric();
        return true;
    default:
        return false;
    }
}

//===========================================================================
bool DumpReader::value(int64_t val) {
    return value((double) val);
}

//===========================================================================
bool DumpReader::value(uint64_t val) {
    switch (m_state) {
    case State::kMetricCreation:
        m_ex.creation = TimePoint{Duration{val}};
        m_state = State::kMetricRetention;
        return true;
    case State::kMetricRetention:
        m_ex.retention = Duration{val};
        m_state = State::kMetricInterval;
        return true;
    case State::kMetricInterval:
        m_ex.interval = Duration{val};
        m_state = State::kMetricFirstTime;
        return true;
    case State::kMetricFirstTime:
        m_ex.first = TimePoint{Duration{val}};
        s_progress.metrics += 1;
        if (!onDumpSeries(m_ex))
            return false;
        m_state = State::kStartSamplesArray;
        return true;
    default:
        return value((double) val);
    }
}

//===========================================================================
bool DumpReader::nextMetric() {
    if (!--m_metrics) {
        m_state = State::kDone;
    } else {
        m_state = State::kStartMetricArray;
    }
    return true;
}


/****************************************************************************
*
*   DbWriter
*
***/

//===========================================================================
bool DbWriter::onDumpMetrics(size_t totalMetrics) {
    return true;
}

//===========================================================================
bool DbWriter::onDumpSeries(const DbSeriesInfoEx & ex) {
    if (appStopping())
        return false;

    dbInsertMetric(&m_id, s_db, ex.name);
    DbMetricInfo info;
    info.creation = ex.creation;
    info.type = ex.type;
    info.retention = ex.retention;
    info.interval = ex.interval;
    dbUpdateMetric(s_db, m_id, info);
    m_time = ex.first;
    m_interval = ex.interval;
    return true;
}

//===========================================================================
bool DbWriter::onDumpSample(double value) {
    dbUpdateSample(s_db, m_id, m_time, value);
    m_time += m_interval;
    return true;
}

//===========================================================================
void DbWriter::onDumpEnd() {
    dbClose(s_db);
    s_db = {};
    if (logGetMsgCount(kLogTypeError)) {
        appSignalShutdown(EX_DATAERR);
    } else {
        tcLogShutdown(&s_progress);
        appSignalShutdown();
    }
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    if (s_db)
        shutdownIncomplete();
}


/****************************************************************************
*
*   Command line
*
***/

static void loadCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("load")
        .desc("Load metrics from dump file into database.")
        .action(loadCmd);
    cli.opt(&database, "[database]")
        .desc("Target database")
        .require();
    cli.opt(&dumpfile, "[input file]")
        .desc("File to load (default extension: .tsdump)");
    cli.opt(&truncate, "truncate", false)
        .desc("Completely replace database contents");
}


/****************************************************************************
*
*   Load command
*
***/

//===========================================================================
static void loadCmd(Cli & cli) {
    s_opts.dumpfile.defaultExt("tsdump");

    logMsgInfo() << "Loading " << s_opts.dumpfile
        << " into " << s_opts.database;
    tcLogStart();
    EnumFlags flags = fDbOpenCreat;
    if (s_opts.truncate)
        flags |= fDbOpenTrunc;
    auto h = dbOpen(s_opts.database, 0, flags);
    if (!h) 
        return cli.fail(EX_ABORTED, "Canceled");

    DbConfig conf = {};
    conf.checkpointMaxData = 1'000'000'000;
    conf.checkpointMaxInterval = 24h;
    dbConfigure(h, conf);
    s_db = h;
    fileSize(&s_progress.totalBytes, s_opts.dumpfile);
    fileStreamBinary(&s_writer, s_opts.dumpfile, envMemoryConfig().pageSize);

    cli.fail(EX_PENDING, "");
}
