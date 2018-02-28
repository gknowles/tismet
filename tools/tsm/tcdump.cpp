// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tcdump.cpp - tsm
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
    string query;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;
static DbProgressInfo s_progress;
static FileAppendStream s_dump;
static CharBuf s_buf;
static MsgPack::Builder s_bld(&s_buf);


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static void appendRest() {
    for (auto && v : s_buf.views()) {
        s_progress.bytes += v.size();
        s_dump.append(v);
    }
    s_buf.clear();
}

//===========================================================================
static void appendIfFull(size_t pending) {
    auto blksize = s_buf.defaultBlockSize();
    if (s_buf.size() + pending > blksize)
        appendRest();
}


/****************************************************************************
*
*   Write dump
*
***/

namespace {

class DumpWriter : public IDbDataNotify {
public:
    bool onDbSeriesStart(const DbSeriesInfo & info) override;
    bool onDbSample(uint32_t id, TimePoint time, double val) override;

private:
    TimePoint m_lastTime;
    TimePoint m_prevTime;
    Duration m_interval;
};

} // namespace

//===========================================================================
bool DumpWriter::onDbSeriesStart(const DbSeriesInfo & info) {
    if (info.infoEx) {
        s_progress.metrics += 1;
        auto & ex = static_cast<const DbSeriesInfoEx &>(info);
        appendIfFull(ex.name.size() + 64);
        s_bld.array(7);
        s_bld.value(ex.name);
        s_bld.value(toString(ex.type));
        s_bld.value(ex.creation.time_since_epoch().count());
        s_bld.value(ex.retention.count());
        s_bld.value(ex.interval.count());
        return true;
    }
    s_bld.value(info.first.time_since_epoch().count());
    auto count = (info.last - info.first) / info.interval;
    s_bld.array(count);
    m_lastTime = info.last;
    m_prevTime = info.first - info.interval;
    m_interval = info.interval;
    return true;
}

//===========================================================================
bool DumpWriter::onDbSample(uint32_t id, TimePoint time, double val) {
    s_progress.samples += 1;
    m_prevTime += m_interval;
    for (; time != m_prevTime; m_prevTime += m_interval)
        s_bld.value(nullptr);
    appendIfFull(8);
    s_bld.value(val);
    return true;
}


/****************************************************************************
*
*   Command line
*
***/

static bool dumpCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("dump")
        .desc("Create dump file from metrics database.")
        .action(dumpCmd);
    cli.opt(&database, "[database]")
        .desc("Database to dump")
        .require();
    cli.opt(&dumpfile, "[output file]")
        .desc("Output defaults to '<dat file>.tsdump', '-' for stdout");
    cli.opt(&query, "f find")
        .desc("Wildcard metric name to match, defaults to all metrics.");
}


/****************************************************************************
*
*   Dump command
*
***/

//===========================================================================
static bool dumpCmd(Cli & cli) {
    tcLogStart();

    FileHandle fout;
    if (!s_opts.dumpfile)
        s_opts.dumpfile.assign(s_opts.database).setExt("tsdump");
    if (s_opts.dumpfile == string_view("-")) {
        fout = fileAttachStdout();
    } else {
        s_opts.dumpfile.defaultExt("tsdump");
        fout = fileOpen(
            s_opts.dumpfile,
            File::fCreat | File::fTrunc | File::fReadWrite
        );
    }

    s_dump.init(10, 2, envMemoryConfig().pageSize);
    if (!fout || !s_dump.attach(fout)) {
        fileClose(fout);
        return cli.fail(
            EX_DATAERR,
            s_opts.dumpfile.str() + ": invalid <outputFile[.tsdump]>"
        );
    }

    logMsgInfo() << "Dumping " << s_opts.database << " to " << s_opts.dumpfile;
    auto h = dbOpen(s_opts.database);
    DumpWriter out;
    UnsignedSet ids;
    dbFindMetrics(&ids, h, s_opts.query);
    s_bld.array(2);
    s_bld.map(1);
    s_bld.element("Tismet Dump Version");
    s_bld.value("2018.1");
    s_bld.array(ids.size());
    DbMetricInfo info;
    for (auto && id : ids) {
        dbGetMetricInfo(&out, h, id);
        dbGetSamples(&out, h, id);
    }
    appendRest();
    s_dump.close();
    dbClose(h);
    tcLogShutdown(&s_progress);
    return true;
}
