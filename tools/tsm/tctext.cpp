// Copyright Glen Knowles 2018 - 2023.
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

class TextWriter : public DbWal::IApplyNotify, public DbWal::IPageNotify {
public:
    TextWriter(ostream & os);

private:
    // Inherited via IApplyNotify
    void onWalApplyCheckpoint(Lsn lsn, Lsn startLsn) override;
    void onWalApplyBeginTxn(Lsn lsn, LocalTxn localTxn) override;
    void onWalApplyCommitTxn(Lsn lsn, LocalTxn localTxn) override;
    void onWalApplyGroupCommitTxn(
        Lsn lsn,
        const vector<LocalTxn> & txns
    ) override;

    void onWalApplyZeroInit(void * ptr) override;
    void onWalApplyRootUpdate(void * ptr, pgno_t rootPage) override;
    void onWalApplyPageFree(void * ptr) override;
    void onWalApplyFullPageInit(
        void * ptr,
        DbPageType type,
        uint32_t id,
        std::span<const uint8_t> data
    ) override;
    void onWalApplyRadixInit(
        void * ptr,
        uint32_t id,
        uint16_t height,
        const pgno_t * firstPgno,
        const pgno_t * lastPgno
    ) override;
    void onWalApplyRadixErase(
        void * ptr,
        size_t firstPos,
        size_t lastPos
    ) override;
    void onWalApplyRadixPromote(void * ptr, pgno_t refPage) override;
    void onWalApplyRadixUpdate(
        void * ptr,
        size_t pos,
        pgno_t refPage
    ) override;
    void onWalApplyBitInit(
        void * ptr,
        uint32_t id,
        uint32_t base,
        bool fill,
        uint32_t pos
    ) override;
    void onWalApplyBitUpdate(
        void * ptr,
        uint32_t firstPos,
        uint32_t lastPos,
        bool value
    ) override;
    void onWalApplyMetricInit(
        void * ptr,
        uint32_t id,
        string_view name,
        TimePoint creation,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onWalApplyMetricUpdate(
        void * ptr,
        TimePoint creation,
        DbSampleType sampleType,
        Duration retention,
        Duration interval
    ) override;
    void onWalApplyMetricClearSamples(void * ptr) override;
    void onWalApplyMetricUpdateSamples(
        void * ptr,
        size_t pos,
        TimePoint refTime,
        size_t refSample,
        pgno_t refPage
    ) override;
    void onWalApplySampleInit(
        void * ptr,
        uint32_t id,
        DbSampleType sampleType,
        TimePoint pageTime,
        size_t lastSample,
        double fill
    ) override;
    void onWalApplySampleUpdate(
        void * ptr,
        size_t firstPos,
        size_t lastPos,
        double value,
        bool updateLast
    ) override;
    void onWalApplySampleUpdateTime(void * ptr, TimePoint pageTime) override;

    // Inherited via IPageNotify
    void * onWalGetPtrForUpdate(
        pgno_t pgno,
        Lsn lsn,
        LocalTxn localTxn
    ) override;
    void onWalUnlockPtr(pgno_t pgno) override;
    void * onWalGetPtrForRedo(
        pgno_t pgno,
        Lsn lsn,
        LocalTxn localTxn
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
void TextWriter::onWalApplyCheckpoint(Lsn lsn, Lsn startLsn) {
    m_os << lsn << '.' << 0 << ": CHECKPOINT = " << startLsn << "\n";
}

//===========================================================================
void TextWriter::onWalApplyBeginTxn(Lsn lsn, LocalTxn localTxn) {
    m_os << lsn << '.' << localTxn << ": txn.begin\n";
}

//===========================================================================
void TextWriter::onWalApplyCommitTxn(Lsn lsn, LocalTxn localTxn) {
    m_os << lsn << '.' << localTxn << ": txn.commit\n";
}

//===========================================================================
void TextWriter::onWalApplyGroupCommitTxn(
    Lsn lsn,
    const vector<LocalTxn> & txns
) {
    m_os << lsn;
    for (auto&& txn : txns)
        m_os << '.' << txn;
    m_os << ": txn.commit (group)\n";
}

//===========================================================================
void TextWriter::onWalApplyZeroInit(void * ptr) {
    out(ptr) << "zero.init\n";
}

//===========================================================================
void TextWriter::onWalApplyRootUpdate(void * ptr, pgno_t rootPage) {
    out(ptr) << "zero.metaRoot = " << rootPage << '\n';
}

//===========================================================================
void TextWriter::onWalApplyPageFree(void * ptr) {
    out(ptr) << "page.free\n";
}

//===========================================================================
void TextWriter::onWalApplyFullPageInit(
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
void TextWriter::onWalApplyRadixInit(
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
void TextWriter::onWalApplyRadixErase(
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
void TextWriter::onWalApplyRadixPromote(void * ptr, pgno_t refPage) {
    out(ptr) << "radix.promote(@" << refPage << ")\n";
}

//===========================================================================
void TextWriter::onWalApplyRadixUpdate(
    void * ptr,
    size_t pos,
    pgno_t refPage
) {
    out(ptr) << "radix[" << pos << "] = @" << refPage << '\n';
}

//===========================================================================
void TextWriter::onWalApplyBitInit(
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
void TextWriter::onWalApplyBitUpdate(
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
void TextWriter::onWalApplyMetricInit(
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
void TextWriter::onWalApplyMetricUpdate(
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
void TextWriter::onWalApplyMetricClearSamples(void * ptr) {
    out(ptr) << "metric.samples.clear\n";
}

//===========================================================================
void TextWriter::onWalApplyMetricUpdateSamples(
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
void TextWriter::onWalApplySampleInit(
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
void TextWriter::onWalApplySampleUpdate(
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
void TextWriter::onWalApplySampleUpdateTime(void * ptr, TimePoint pageTime) {
    out(ptr) << "samples.time = " << pageTime << '\n';
}

//===========================================================================
void * TextWriter::onWalGetPtrForUpdate(
    pgno_t pgno,
    Lsn lsn,
    LocalTxn localTxn
) {
    assert(!"updates not supported when dumping wal");
    return nullptr;
}

//===========================================================================
void TextWriter::onWalUnlockPtr(pgno_t pgno) {
    assert(!"updates not supported when dumping wal");
}

//===========================================================================
void * TextWriter::onWalGetPtrForRedo(
    pgno_t pgno,
    Lsn lsn,
    LocalTxn localTxn
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
    DbWal wal(&writer, &writer);
    if (wal.open(s_opts.tslfile, fDbOpenReadOnly)) {
        EnumFlags flags = DbWal::fRecoverIncompleteTxns;
        if (s_opts.all)
            flags |= DbWal::fRecoverBeforeCheckpoint;
        wal.recover(flags);
        wal.close();
    }
    tcLogShutdown(&s_progress);
}
