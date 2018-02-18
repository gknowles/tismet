// Copyright Glen Knowles 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tctext.cpp - tsm
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
    Path tslfile;
    Path ofile;

    CmdOpts();
};

class TextWriter : public DbLog::IApplyNotify, public DbLog::IPageNotify {
public:
    TextWriter(ostream & os);

private:
    // Inherited via IApplyNotify
    void onLogApplyZeroInit(void * ptr) override;
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplySegmentUpdate(
        void * ptr,
        uint32_t refPage,
        bool free
    ) override;
    void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const uint32_t * firstPgno,
        const uint32_t * lastPgno
    ) override;
    void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onLogApplyRadixPromote(void * ptr, uint32_t refPage) override;
    void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        uint32_t refPage
    ) override;
    void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        string_view name,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onLogApplyMetricUpdate(
        void * ptr,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onLogApplyMetricClearSamples(void * ptr) override;
    void onLogApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        uint32_t refPage,
        TimePoint refTime,
        bool updateIndex
    ) override;
    void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        TimePoint pageTime,
        size_t lastSample
    ) override;
    void onLogApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) override;
    void onLogApplySampleUpdateTime(void * ptr, TimePoint pageTime) override;

    // Inherited via IPageNotify
    void * onLogGetUpdatePtr(uint64_t lsn, uint32_t pgno) override;
    void * onLogGetRedoPtr(uint64_t lsn, uint32_t pgno) override;

    ostream & out(void * ptr);
    string_view timeStr(TimePoint time);

    // Data members
    ostream & m_os;
    DbPageHeader m_hdr;
    Time8601Str m_ts;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static DbProgressInfo s_progress;


/****************************************************************************
*
*   TextWriter
*
***/

//===========================================================================
TextWriter::TextWriter(ostream & os)
    : m_os{os}
{}

//===========================================================================
ostream & TextWriter::out(void * ptr) {
    auto hdr = static_cast<DbPageHeader *>(ptr);
    m_os << hdr->lsn << " @" << hdr->pgno << ": ";
    return m_os;
}

//===========================================================================
string_view TextWriter::timeStr(TimePoint time) {
    m_ts.set(time, 0, timeZoneMinutes(time));
    return m_ts.view();
}

//===========================================================================
void TextWriter::onLogApplyZeroInit(void * ptr) {
    out(ptr) << "zero.init\n";
}

//===========================================================================
void TextWriter::onLogApplyPageFree(void * ptr) {
    out(ptr) << "page.free\n";
}

//===========================================================================
void TextWriter::onLogApplySegmentUpdate(
    void * ptr,
    uint32_t refPage,
    bool free
) {
    out(ptr) << "free[@" << refPage << "] = " << (free ? 1 : 0) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyRadixInit(
    void * ptr,
    uint32_t id,
    uint16_t height,
    const uint32_t * firstPgno,
    const uint32_t * lastPgno
) {
    auto & os = out(ptr);
    os << "radix/" << id << ".init = " << height << " height" << '\n';
}

//===========================================================================
void TextWriter::onLogApplyRadixErase(
    void * ptr,
    size_t firstPos,
    size_t lastPos
) {
    out(ptr) << "radix.erase(" << firstPos << " thru " << lastPos << ")\n";
}

//===========================================================================
void TextWriter::onLogApplyRadixPromote(void * ptr, uint32_t refPage) {
    out(ptr) << "radix.promote(@" << refPage << ")\n";
}

//===========================================================================
void TextWriter::onLogApplyRadixUpdate(
    void * ptr,
    size_t pos,
    uint32_t refPage
) {
    out(ptr) << "radix[" << pos << "] = @" << refPage << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricInit(
    void * ptr,
    uint32_t id,
    string_view name,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    out(ptr) << name << "/" << id << ".init = "
        << toString(sampleType, "UNKNOWN_TYPE") << ", "
        << toString(retention, DurationFormat::kTwoPart) << ", "
        << toString(interval, DurationFormat::kTwoPart) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricUpdate(
    void * ptr,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    out(ptr) << "metric = "
        << toString(sampleType, "UNKNOWN_TYPE") << ", "
        << toString(retention, DurationFormat::kTwoPart) << ", "
        << toString(interval, DurationFormat::kTwoPart) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricClearSamples(void * ptr) {
    out(ptr) << "metric.samples.clear\n";
}

//===========================================================================
void TextWriter::onLogApplyMetricUpdateSamples(
    void * ptr,
    size_t pos,
    uint32_t refPage,
    TimePoint refTime,
    bool updateIndex
) {
    out(ptr) << "metric.samples[" << pos << "] = "
        << "@" << refPage << " / " << timeStr(refTime)
        << (updateIndex ? ", update index" : "")
        << '\n';
}

//===========================================================================
void TextWriter::onLogApplySampleInit(
    void * ptr,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample
) {
    out(ptr) << "samples/" << id << ".init = "
        << toString(sampleType, "UNKNOWN_TYPE") << ", "
        << timeStr(pageTime) << ", "
        << lastSample << "\n";
}

//===========================================================================
void TextWriter::onLogApplySampleUpdate(
    void * ptr,
    size_t firstPos,
    size_t lastPos,
    double value,
    bool updateLast
) {
    out(ptr) << "samples[" << firstPos << ", " << lastPos << "] = "
        << value << (updateLast ? ", update last" : "")
        << '\n';
}

//===========================================================================
void TextWriter::onLogApplySampleUpdateTime(void * ptr, TimePoint pageTime) {
    out(ptr) << "samples.time = " << timeStr(pageTime) << '\n';
}

//===========================================================================
void * TextWriter::onLogGetUpdatePtr(uint64_t lsn, uint32_t pgno) {
    return nullptr;
}

//===========================================================================
void * TextWriter::onLogGetRedoPtr(uint64_t lsn, uint32_t pgno) {
    m_hdr.lsn = lsn;
    m_hdr.pgno = pgno;
    return &m_hdr;
}


/****************************************************************************
*
*   Command line
*
***/

static bool textCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("text")
        .desc("Translate write ahead log (wal) file to human readable text.")
        .action(textCmd);
    cli.opt(&tslfile, "[wal file]")
        .desc("Wal file to dump, extension defaults to '.tsl'");
    cli.opt(&ofile, "[output file]")
        .desc("Output defaults to '<dat file>.txt', '-' for stdout");
}


/****************************************************************************
*
*   Text command
*
***/

//===========================================================================
static bool textCmd(Cli & cli) {
    if (!s_opts.tslfile)
        return cli.badUsage("No value given for <wal file[.tsl]>");
    s_opts.tslfile.defaultExt("tsl");

    ostream * os{nullptr};
    ofstream ofile;
    if (!s_opts.ofile)
        s_opts.ofile.assign(s_opts.tslfile).setExt("txt");
    if (s_opts.ofile == string_view("-")) {
        os = &cout;
    } else {
        ofile.open(s_opts.ofile.str(), ios::trunc);
        if (!ofile) {
            return cli.fail(
                EX_DATAERR,
                string(s_opts.ofile) + ": invalid <outputFile[.txt]>"
            );
        }
        os = &ofile;
    }

    logMsgInfo() << "Dumping " << s_opts.tslfile << " to " << s_opts.ofile;
    tcLogStart();
    TextWriter writer(*os);
    DbLog dlog(&writer, &writer);
    dlog.open(s_opts.tslfile, 0, fDbOpenReadOnly | fDbOpenIncludeIncompleteTxns);
    dlog.close();
    tcLogShutdown(&s_progress);

    return true;
}
