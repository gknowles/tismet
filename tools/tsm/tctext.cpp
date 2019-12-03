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
    bool all;

    CmdOpts();
};

class TextWriter : public DbLog::IApplyNotify, public DbLog::IPageNotify {
public:
    TextWriter(ostream & os);

private:
    // Inherited via IApplyNotify
    void onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn) override;
    void onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn) override;
    void onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn) override;

    void onLogApplyZeroInit(void * ptr) override;
    void onLogApplyZeroUpdateRoots(
        void * ptr,
        pgno_t infoRoot,
        pgno_t nameRoot,
        pgno_t idRoot
    ) override;
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplySegmentUpdate(
        void * ptr,
        pgno_t refPage,
        bool free
    ) override;
    void onLogApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) override;
    void onLogApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onLogApplyRadixPromote(void * ptr, pgno_t refPage) override;
    void onLogApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) override;
    void onLogApplyIndexLeafInit(void * ptr, uint32_t id) override;
    void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        TimePoint creation,
        Duration retention
    ) override;
    void onLogApplyMetricUpdate(
        void * ptr,
        TimePoint creation,
        Duration retention
    ) override;
    void onLogApplyMetricEraseSamples(
        void * ptr,
        size_t count,
        TimePoint lastIndexTime
    ) override;
    void onLogApplyMetricUpdateSample(
        void * ptr,
        size_t pos,
        double value,
        double dv
    ) override;
    void onLogApplyMetricInsertSample(
        void * ptr,
        size_t pos,
        Duration dt,
        double value,
        double dv
    ) override;
    void onLogApplySampleInit(void * ptr, uint32_t id) override;
    void onLogApplySampleUpdate(
        void * ptr,
        size_t offset,
        string_view data,
        size_t unusedBits
    ) override;

    // Inherited via IPageNotify
    void * onLogGetUpdatePtr(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) override;
    void * onLogGetRedoPtr(
        pgno_t pgno,
        uint64_t lsn,
        uint16_t localTxn
    ) override;

    ostream & out(void * ptr);

    // Data members
    ostream & m_os;
    DbPageHeader m_hdr;
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
    if (hdr->pgno >= (unsigned) numeric_limits<int32_t>::max())
        logMsgError() << "Data page out of range: @" << hdr->pgno;
    m_os << hdr->lsn
        << '.' << hdr->checksum // localTxn smuggled through as checksum
        << " @" << hdr->pgno << ": ";
    return m_os;
}

//===========================================================================
void TextWriter::onLogApplyCommitCheckpoint(uint64_t lsn, uint64_t startLsn) {
    m_os << lsn << '.' << 0 << ": CHECKPOINT.commit = " << startLsn << "\n";
}

//===========================================================================
void TextWriter::onLogApplyBeginTxn(uint64_t lsn, uint16_t localTxn) {
    m_os << lsn << '.' << localTxn << ": txn.begin\n";
}

//===========================================================================
void TextWriter::onLogApplyCommitTxn(uint64_t lsn, uint16_t localTxn) {
    m_os << lsn << '.' << localTxn << ": txn.commit\n";
}

//===========================================================================
void TextWriter::onLogApplyZeroInit(void * ptr) {
    out(ptr) << "zero.init\n";
}

//===========================================================================
void TextWriter::onLogApplyZeroUpdateRoots(
    void * ptr,
    pgno_t infoRoot,
    pgno_t nameRoot,
    pgno_t idRoot
) {
    out(ptr) << "zero.roots = "
        << infoRoot << ", " << nameRoot << ", " << idRoot;
}

//===========================================================================
void TextWriter::onLogApplyPageFree(void * ptr) {
    out(ptr) << "page.free\n";
}

//===========================================================================
void TextWriter::onLogApplySegmentUpdate(
    void * ptr,
    pgno_t refPage,
    bool free
) {
    out(ptr) << "free[@" << refPage << "] = " << (free ? 1 : 0) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyRadixInit(
    void * ptr,
    uint32_t id,
    uint16_t height,
    const pgno_t * firstPgno,
    const pgno_t * lastPgno
) {
    auto & os = out(ptr);
    os << "radix/" << id << ".init = " << height << '\n';
}

//===========================================================================
void TextWriter::onLogApplyRadixErase(
    void * ptr,
    size_t firstPos,
    size_t lastPos
) {
    auto & os = out(ptr);
    os << "radix[" << firstPos;
    if (firstPos != lastPos - 1)
        os << " thru " << lastPos - 1;
    os << "] = 0\n";
}

//===========================================================================
void TextWriter::onLogApplyRadixPromote(void * ptr, pgno_t refPage) {
    out(ptr) << "radix.promote(@" << refPage << ")\n";
}

//===========================================================================
void TextWriter::onLogApplyRadixUpdate(
    void * ptr,
    size_t pos,
    pgno_t refPage
) {
    out(ptr) << "radix[" << pos << "] = @" << refPage << '\n';
}

//===========================================================================
void TextWriter::onLogApplyIndexLeafInit(void * ptr, uint32_t id) {
    out(ptr) << "index/" << id << ".init";
}

//===========================================================================
void TextWriter::onLogApplyMetricInit(
    void * ptr,
    uint32_t id,
    TimePoint creation,
    Duration retention
) {
    out(ptr) << "metric/" << id << ".init = "
        << creation << ", "
        << toString(retention, DurationFormat::kTwoPart) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricUpdate(
    void * ptr,
    TimePoint creation,
    Duration retention
) {
    out(ptr) << "metric = "
        << creation << ", "
        << toString(retention, DurationFormat::kTwoPart) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricEraseSamples(
    void * ptr,
    size_t count,
    TimePoint lastIndexTime
) {
    out(ptr) << "metric.samples.erase(0, " << count << "); "
        << "metric.lastIndexTime = " << lastIndexTime << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricUpdateSample(
    void * ptr,
    size_t pos,
    double value,
    double dv
) {
    auto & os = out(ptr);
    os << "metric.samples[" << pos << "].value ";
    if (isnan(value)) {
        os << "= " << value << '\n';
    } else {
        os << "+= " << dv << '\n';
    }
}

//===========================================================================
void TextWriter::onLogApplyMetricInsertSample(
    void * ptr,
    size_t pos,
    Duration dt,
    double value,
    double dv
) {
    auto & os = out(ptr);
    os << "metric.samples[" << pos << "] ";
    if (isnan(value)) {
        os << "= " << toString(dt, DurationFormat::kTwoPart) << ", "
            << value << '\n';
    } else {
        os << "+= " << toString(dt, DurationFormat::kTwoPart)
            << dv << '\n';
    }
}

//===========================================================================
void TextWriter::onLogApplySampleInit(void * ptr, uint32_t id) {
    out(ptr) << "samples/" << id << ".init\n";
}

//===========================================================================
void TextWriter::onLogApplySampleUpdate(
    void * ptr,
    size_t offset,
    string_view data,
    size_t unusedBits
) {
    auto & os = out(ptr);
    os << "samples[" << offset << " thru " << offset + data.size() << "] = "
        << "<" << data.size() << " bytes>\n";
}

//===========================================================================
void * TextWriter::onLogGetUpdatePtr(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    assert(!"updates not supported when dumping wal");
    return nullptr;
}

//===========================================================================
void * TextWriter::onLogGetRedoPtr(
    pgno_t pgno,
    uint64_t lsn,
    uint16_t localTxn
) {
    m_hdr.checksum = localTxn;
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
    cli.opt(&all, "a all")
        .desc("Include all logs entries instead of just those after the last "
            "checkpoint.");
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

    ostream * os = nullptr;
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
    dlog.open(s_opts.tslfile, 0, fDbOpenReadOnly);
    auto flags = DbLog::fRecoverIncompleteTxns;
    if (s_opts.all)
        flags |= DbLog::fRecoverBeforeCheckpoint;
    dlog.recover(flags);
    dlog.close();
    tcLogShutdown(&s_progress);

    return true;
}
