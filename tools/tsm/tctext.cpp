// Copyright Glen Knowles 2018 - 2022.
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
    void onLogApplyPageFree(void * ptr) override;
    void onLogApplyFullPage(
        void * ptr,
        DbPageType type,
        uint32_t id,
        std::span<const uint8_t> data
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
    void onLogApplyBitInit(
        void * ptr,
        uint32_t id,
        uint32_t base,
        bool fill,
        uint32_t pos
    ) override;
    void onLogApplyBitUpdate(
        void * ptr,
        uint32_t firstPos,
        uint32_t lastPos,
        bool value
    ) override;
    void onLogApplyMetricInit(
        void * ptr,
        uint32_t id,
        string_view name,
        TimePoint creation,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onLogApplyMetricUpdate(
        void * ptr,
        TimePoint creation,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onLogApplyMetricClearSamples(void * ptr) override;
    void onLogApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) override;
    void onLogApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        TimePoint pageTime,
        size_t lastSample,
        double fill
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
void TextWriter::onLogApplyPageFree(void * ptr) {
    out(ptr) << "page.free\n";
}

//===========================================================================
void TextWriter::onLogApplyFullPage(
    void * ptr,
    DbPageType type,
    uint32_t id,
    std::span<const uint8_t> data
) {
    auto & os = out(ptr);
    os << "page/" << id << ".full " << toString(type) << ", " 
        << data.size() << " bytes\n";
    hexDump(os, {(char *) data.data(), data.size()});
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
void TextWriter::onLogApplyBitInit(
    void * ptr,
    uint32_t id,
    uint32_t base,
    bool fill,
    uint32_t pos
) {
    auto & os = out(ptr);
    os << "bit/" << id << ".init[" << base << "] = " << (fill ? 1 : 0);
    if (pos != numeric_limits<uint32_t>::max())
        os << ", bit[" << pos << "] = " << (fill ? 0 : 1);
    os << '\n';
}

//===========================================================================
void TextWriter::onLogApplyBitUpdate(
    void * ptr,
    uint32_t firstPos,
    uint32_t lastPos,
    bool value
) {
    auto & os = out(ptr);
    os << "bit[" << firstPos;
    if (lastPos - firstPos > 1) 
        os << "," << lastPos;
    os << "] = " << (value ? 1 : 0) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricInit(
    void * ptr,
    uint32_t id,
    string_view name,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    out(ptr) << name << "/" << id << ".init = "
        << creation << ", "
        << toString(sampleType, "UNKNOWN_TYPE") << ", "
        << toString(retention, DurationFormat::kTwoPart) << ", "
        << toString(interval, DurationFormat::kTwoPart) << '\n';
}

//===========================================================================
void TextWriter::onLogApplyMetricUpdate(
    void * ptr,
    TimePoint creation,
    DbSampleType sampleType,
    Duration retention,
    Duration interval
) {
    out(ptr) << "metric = "
        << creation << ", "
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
    TimePoint refTime,
    size_t refSample,
    pgno_t refPage
) {
    auto & os = out(ptr);
    if (refPage)
        os << "metric.samples[" << pos << "] = @" << refPage << "; ";
    os << "metric.samples.last = ";
    if (!empty(refTime))
        os << pos << " / ";
    if (refPage)
        os << "@" << refPage;
    if (refSample != (size_t) -1)
        os << '.' << refSample;
    if (!empty(refTime))
        os << " / " << refTime;
    os << '\n';
}

//===========================================================================
void TextWriter::onLogApplySampleInit(
    void * ptr,
    uint32_t id,
    DbSampleType sampleType,
    TimePoint pageTime,
    size_t lastSample,
    double fill
) {
    out(ptr) << "samples/" << id << ".init = " << fill << ", "
        << toString(sampleType, "UNKNOWN_TYPE") << ", "
        << pageTime << ", "
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
    auto & os = out(ptr);
    os << "samples[" << firstPos;
    if (isnan(value)) {
        if (firstPos < lastPos - 1)
            os << " thru " << lastPos - 1;
        os << "] = NAN";
    } else {
        if (firstPos < lastPos) {
            os << " thru " << lastPos - 1 << ", " << lastPos << "] = NAN, ";
        } else {
            os << "] = ";
        }
        os << value;
    }
    if (updateLast)
        os << "; samples.last = " << lastPos;
    os << '\n';
}

//===========================================================================
void TextWriter::onLogApplySampleUpdateTime(void * ptr, TimePoint pageTime) {
    out(ptr) << "samples.time = " << pageTime << '\n';
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

static void textCmd(Cli & cli);

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
static void textCmd(Cli & cli) {
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
    dlog.open(s_opts.tslfile, fDbOpenReadOnly);
    EnumFlags flags = DbLog::fRecoverIncompleteTxns;
    if (s_opts.all)
        flags |= DbLog::fRecoverBeforeCheckpoint;
    dlog.recover(flags);
    dlog.close();
    tcLogShutdown(&s_progress);
}
