// Copyright Glen Knowles 2017 - 2022.
// Distributed under the Boost Software License, Version 1.0.
//
// tcgen.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace std::chrono;
using namespace Dim;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct CmdOpts {
    enum OutputType {
        kInvalidOutput,
        kFileOutput,
        kAddrOutput,
    };
    OutputType otype{};
    Path ofile;
    string oaddr;
    DbProgressInfo progress;
    unsigned totalSecs;

    string prefix;
    unsigned metrics;
    unsigned intervalSecs;
    double minDelta;
    double maxDelta;

    TimePoint startTime;
    TimePoint endTime;

    CmdOpts();
};

struct Metric {
    string name;
    double value;
    TimePoint time;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static SockMgrHandle s_mgr;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static bool checkLimits(size_t moreBytes) {
    // Update counters and check thresholds, if exceeded roll back last value.
    s_opts.progress.bytes += moreBytes;
    s_opts.progress.samples += 1;
    if (s_opts.progress.totalBytes
            && s_opts.progress.bytes > s_opts.progress.totalBytes
        || s_opts.progress.totalSamples
            && s_opts.progress.samples > s_opts.progress.totalSamples
    ) {
        s_opts.progress.bytes -= moreBytes;
        s_opts.progress.samples -= 1;
        return false;
    }
    return true;
}


/****************************************************************************
*
*   MetricSource
*
***/

namespace {

class MetricSource {
public:
    MetricSource();
    Metric * next();

private:
    random_device m_rdev;
    default_random_engine m_reng;
    uniform_real_distribution<> m_rdist;

    vector<Metric> m_metrics;
    size_t m_pos = (size_t) -1;
    bool m_firstPass{true};
};

} // namespace

//===========================================================================
MetricSource::MetricSource()
    : m_reng{m_rdev()}
    , m_rdist{s_opts.minDelta, s_opts.maxDelta}
    , m_metrics{s_opts.metrics}
{
    static const char * numerals[] = {
        "zero.", "one.", "two.", "three.", "four.",
        "five.", "six.", "seven.", "eight.", "nine.",
    };
    StrFrom<unsigned> str{0};
    for (unsigned i = 0; i < s_opts.metrics; ++i) {
        auto & met = m_metrics[i];
        if (!s_opts.prefix.empty())
            met.name += s_opts.prefix;
        for (auto && ch : str.set(i))
            met.name += numerals[ch - '0'];
        // remove extra trailing dot
        met.name.pop_back();

        met.value = 0;
        met.time = s_opts.startTime;
    }
}

//===========================================================================
Metric * MetricSource::next() {
    if (m_metrics.empty())
        return nullptr;
    if (++m_pos == m_metrics.size()) {
        m_pos = 0;
        m_firstPass = false;
    }
    auto & met = m_metrics[m_pos];
    if (m_firstPass)
        return &met;

    // advance metric
    if (s_opts.endTime.time_since_epoch().count()
        && met.time >= s_opts.endTime
    ) {
        m_metrics.clear();
        return nullptr;
    }
    met.time += (seconds) s_opts.intervalSecs;
    met.value += m_rdist(m_reng);
    return &met;
}


/****************************************************************************
*
*   BufferSource
*
***/

namespace {

class BufferSource {
public:
    size_t next(void * out, size_t outLen, MetricSource & src);

private:
    string m_buffer;
};

} // namespace

//===========================================================================
size_t BufferSource::next(void * out, size_t outLen, MetricSource & src) {
    auto base = (char *) out;
    auto ptr = base;
    for (;;) {
        if (auto len = m_buffer.size()) {
            if (len > outLen) {
                memcpy(ptr, m_buffer.data(), outLen);
                m_buffer.erase(0, outLen);
                return ptr - base + outLen;
            }
            memcpy(ptr, m_buffer.data(), len);
            ptr += len;
            outLen -= len;
            m_buffer.clear();
        }

        auto met = src.next();
        if (!met)
            return ptr - base;
        carbonWrite(m_buffer, met->name, met->time, (float) met->value);

        // Check thresholds, if exceeded roll back last value and ensure
        // subsequent calls return 0 bytes.
        if (!checkLimits(m_buffer.size())) {
            m_buffer.clear();
            return ptr - base;
        }
    }
}


/****************************************************************************
*
*   AddrConn
*
***/

namespace {

class AddrConn : public IAppSocketNotify {
public:
    static constexpr size_t kBufferSize = 4096;

public:
    // Inherited via IAppSocketNotify
    void onSocketConnect(const AppSocketInfo & info) override;
    void onSocketConnectFailed() override;
    void onSocketDisconnect() override;
    bool onSocketRead(AppSocketData & data) override;
    void onSocketBufferChanged(const AppSocketBufferInfo & info) override;
private:
    void write();

    MetricSource m_mets;
    BufferSource m_bufs;
    bool m_done{false};
    bool m_full{false};
};

} // namespace

//===========================================================================
void AddrConn::write() {
    string buffer;
    for (;;) {
        buffer.resize(kBufferSize);
        auto len = m_bufs.next(buffer.data(), buffer.size(), m_mets);
        if (!len)
            break;
        buffer.resize(len);
        socketWrite(this, buffer);
        if (m_full || m_done)
            return;
    }
    m_done = true;
}

//===========================================================================
void AddrConn::onSocketConnect(const AppSocketInfo & info) {
    write();
}

//===========================================================================
void AddrConn::onSocketConnectFailed() {
    logMsgInfo() << "Connect failed";
    appSignalShutdown();
}

//===========================================================================
void AddrConn::onSocketDisconnect() {
    if (!m_done) {
        logMsgInfo() << "Disconnect";
        m_done = true;
    }
    sockMgrSetAddresses(s_mgr, nullptr, 0);
    appSignalShutdown();
}

//===========================================================================
bool AddrConn::onSocketRead(AppSocketData & data) {
    return true;
}

//===========================================================================
void AddrConn::onSocketBufferChanged(const AppSocketBufferInfo & info) {
    if (info.waiting) {
        m_full = true;
    } else if (m_full && !info.waiting) {
        m_full = false;
        write();
    } else if (m_done
        && !info.incomplete
        && info.total == s_opts.progress.bytes
    ) {
        tcLogShutdown(&s_opts.progress);
        appSignalShutdown();
    }
}


/****************************************************************************
*
*   AddrJob
*
***/

namespace {

class AddrJob : ISockAddrNotify {
public:
    bool start(Cli & cli);

private:
    // Inherited via ISockAddrNotify
    void onSockAddrFound(const SockAddr ptr[], int count) override;

    int m_cancelId;
};

} // namespace

//===========================================================================
bool AddrJob::start(Cli & cli) {
    s_mgr = sockMgrConnect<AddrConn>("Metric Out");
    addressQuery(&m_cancelId, this, s_opts.oaddr, 2003);
    cli.fail(EX_PENDING, "");
    return true;
}

//===========================================================================
void AddrJob::onSockAddrFound(const SockAddr ptr[], int count) {
    if (!count) {
        appSignalShutdown();
    } else {
        logMsgInfo() << "Writing to " << s_opts.oaddr << " (" << *ptr << ")";
        tcLogStart(&s_opts.progress);
        sockMgrSetAddresses(s_mgr, ptr, count);
    }
    delete this;
}


/****************************************************************************
*
*   FileJob
*
***/

namespace {

class FileJob : public ITaskNotify {
public:
    ~FileJob();

    bool start(Cli & cli);

private:
    // Inherited via ITaskNotify
    void onTask() override;

    FileAppendStream m_file{100, 2, envMemoryConfig().pageSize};
    MetricSource m_mets;
};

} // namespace

//===========================================================================
FileJob::~FileJob() {
    m_file.close();
    tcLogShutdown(&s_opts.progress);
    appSignalShutdown();
}

//===========================================================================
bool FileJob::start(Cli & cli) {
    auto fname = s_opts.ofile;
    if (!fname)
        return cli.badUsage("No value given for <output file[.txt]>");
    if (fname.view() != "-") {
        if (!m_file.open(fname.defaultExt("txt"), FileAppendStream::kTrunc)) {
            return cli.fail(
                EX_DATAERR,
                fname.str() + ": open <outputFile[.txt]> failed"
            );
        }
    }

    taskSetQueueThreads(taskComputeQueue(), 2);
    logMsgInfo() << "Writing to " << fname;
    tcLogStart(&s_opts.progress);
    taskPushCompute(this);
    cli.fail(EX_PENDING, "");
    return true;
}

//===========================================================================
void FileJob::onTask() {
    string buf;
    for (;;) {
        auto met = m_mets.next();
        if (!met)
            break;
        carbonWrite(buf, met->name, met->time, (float) met->value);
        if (!checkLimits(buf.size()))
            break;
        if (m_file) {
            m_file.append(buf);
        } else {
            cout << buf;
        }
        buf.clear();
    }
    delete this;
}


/****************************************************************************
*
*   Command line
*
***/

static bool genCmd(Cli & cli);

// 2001-01-01 12:00:00 UTC
constexpr TimePoint kDefaultStartTime{12'622'824'000s};

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("gen")
        .desc("Generate test metrics.")
        .action(genCmd)
        .group("Target").sortKey("1")
        .title("Output Target (exactly one target is required)");
    cli.opt(&ofile, "F file")
        .desc("Output file, '-' for stdout, extension defaults to '.txt'")
        .check([&](auto&, auto&, auto&) { return otype = kFileOutput; })
        .after([&](auto & cli, auto&, auto&) {
            return otype
                || cli.badUsage("No output target given.");
        });
    cli.opt(&oaddr, "A addr")
        .desc("Socket address to receive metrics, port defaults to 2003")
        .valueDesc("ADDRESS")
        .check([&](auto&, auto&, auto&) { return otype = kAddrOutput; });

    cli.group("~").title("Other");

    cli.group("When to Stop").sortKey("2");
    cli.opt(&progress.totalBytes, "B bytes", 0)
        .desc("Max bytes to generate, 0 for unlimited");
    //cli.opt(&totalSecs, "T time", 0)
    //    .desc("Max seconds to run, 0 for unlimited");
    cli.opt(&progress.totalSamples, "S samples", 10)
        .desc("Max samples to generate, 0 for unlimited");

    cli.group("Metrics to Generate").sortKey("3");
    cli.opt(&prefix, "prefix", "test.")
        .desc("Prefix to generated metric names");
    cli.opt(&metrics, "m metrics", 100)
        .range(1, numeric_limits<decltype(metrics)>::max())
        .desc("Number of metrics");
    cli.opt(&startTime, "s start", kDefaultStartTime)
        .desc("Start time of first sample")
        .valueDesc("TIME");
    cli.opt(&endTime, "e end")
        .desc("Time of last sample, rounded up to next interval")
        .valueDesc("TIME");
    cli.opt(&intervalSecs, "i interval", 60)
        .desc("Seconds between samples");
    cli.opt(&minDelta, "dmin", 0.0)
        .desc("Minimum delta between consecutive samples")
        .valueDesc("FLOAT");
    cli.opt(&maxDelta, "dmax", 10.0)
        .desc("Max delta between consecutive samples")
        .valueDesc("FLOAT");
}

//===========================================================================
static bool genCmd(Cli & cli) {
    if (s_opts.otype == CmdOpts::kFileOutput) {
        auto job = make_unique<FileJob>();
        if (job->start(cli))
            job.release();
    } else {
        assert(s_opts.otype == CmdOpts::kAddrOutput);
        auto job = make_unique<AddrJob>();
        if (job->start(cli))
            job.release();
    }
    return false;
}
