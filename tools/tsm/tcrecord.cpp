// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcrecord.cpp - tsm
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
    Path ofile;
    bool trunc;
    bool append;

    uint64_t maxBytes;
    unsigned maxSecs;
    uint64_t maxValues;

    string addrStr;
    Endpoint addr;

    CmdOpts();
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static CmdOpts s_opts;

static uint64_t s_valuesWritten;
static uint64_t s_bytesWritten;

static TimePoint s_startTime;


//===========================================================================
static void logStart(string_view target, const Endpoint & source) {
    s_startTime = Clock::now();
    logMsgInfo() << "Recording " << source << " into " << target;
    if (s_opts.maxBytes || s_opts.maxSecs) {
        auto os = logMsgInfo();
        os.imbue(locale(""));
        os << "Limits";
        if (auto num = s_opts.maxValues) 
            os << "; values: " << num;
        if (auto num = s_opts.maxBytes) 
            os << "; bytes: " << num;
        if (auto num = s_opts.maxSecs)
            os << "; seconds: " << num;
    }
}

//===========================================================================
static void logShutdown() {
    TimePoint finish = Clock::now();
    std::chrono::duration<double> elapsed = finish - s_startTime;
    auto os = logMsgInfo();
    os.imbue(locale(""));
    os << "Done; values: " << s_valuesWritten
        << "; bytes: " << s_bytesWritten
        << "; seconds: " << elapsed.count();
}


/****************************************************************************
*
*   RecordTimer
*
***/

namespace {

class RecordTimer : public ITimerNotify {
    Duration onTimer(TimePoint now) override;
};

} // namespace

static RecordTimer s_timer;

//===========================================================================
Duration RecordTimer::onTimer(TimePoint now) {
    appSignalShutdown();
    return kTimerInfinite;
}


/****************************************************************************
*
*   File
*
***/

static FileHandle s_file;
static size_t s_fpos;
static char * s_obuf;
static size_t s_obufLen;
static string_view s_out;

//===========================================================================
static bool openFile() {
    auto flags = File::fReadWrite | File::fCreat 
        | File::fBlocking | File::fAligned;
    if (s_opts.trunc)
        flags |= File::fTrunc;
    else if (!s_opts.append)
        flags |= File::fExcl;
    s_file = fileOpen(s_opts.ofile, flags);
    if (!s_file) {
        return Cli{}.fail(
            EX_DATAERR, 
            s_opts.ofile.str() + ": open output failed"
        );
    }

    s_obufLen = envMemoryConfig().pageSize;
    s_obuf = (char *) aligned_alloc(s_obufLen, s_obufLen);
    
    s_fpos = fileSize(s_file);
    if (!errno) {
        return Cli{}.fail(
            EX_DATAERR, 
            s_opts.ofile.str() + ": open output failed"
        );
    }
    auto used = s_fpos % s_obufLen;
    s_out = string_view{s_obuf + used, s_obufLen - used};
    s_fpos -= used;
    if (s_fpos) 
        fileReadWait(s_obuf, s_obufLen, s_file, s_fpos);
    return true;
}

//===========================================================================
static void appendFile(string_view buf) {
    if (!s_file) {
        cout.write(buf.data(), buf.size());
        return;
    }
    auto bytes = min(buf.size(), s_out.size());
    memcpy((char *) s_out.data(), buf.data(), bytes);
    s_out.remove_prefix(bytes);
    if (s_out.empty()) {
        fileWriteWait(s_file, s_fpos, s_obuf, s_obufLen);
        s_fpos += s_obufLen;
        s_out = string_view(s_obuf, s_obufLen);
        if (auto left = buf.size() - bytes) {
            memcpy((char *) s_out.data(), buf.data() + bytes, left);
            s_out.remove_prefix(left);
        }
    }
}

//===========================================================================
static void closeFile() {
    if (s_out.size()) {
        fileClose(s_file);
        s_file = fileOpen(s_opts.ofile, File::fReadWrite | File::fBlocking);
        fileAppendWait(s_file, s_obuf, s_obufLen - s_out.size());
    }
    aligned_free(s_obuf);
    fileClose(s_file);
}


/****************************************************************************
*
*   RecordConn
*
***/

namespace {

class RecordConn : public ICarbonSocketNotify {
public:
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(uint32_t id, TimePoint time, double value) override;
private:
    string_view m_name;
    string m_buf;
};

} // namespace

//===========================================================================
uint32_t RecordConn::onCarbonMetric(string_view name) {
    m_name = name;
    return 1;
}

//===========================================================================
void RecordConn::onCarbonValue(uint32_t id, TimePoint time, double value) {
    assert(id == 1);
    m_buf.clear();
    carbonWrite(m_buf, m_name, time, (float) value);
    appendFile(m_buf);
    s_bytesWritten += m_buf.size();
    s_valuesWritten += 1;
    if (appStopping())
        return;
    if (s_opts.maxValues && s_valuesWritten >= s_opts.maxValues
        || s_opts.maxBytes && s_bytesWritten >= s_opts.maxBytes
    ) {
        return appSignalShutdown();
    }
}


/****************************************************************************
*
*   ShutdownNotify
*
***/

namespace {

class ShutdownNotify : public IShutdownNotify {
    void onShutdownClient(bool firstTry) override;
    void onShutdownServer(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownClient(bool firstTry) {
    socketCloseWait<RecordConn>(
        s_opts.addr,
        (AppSocket::Family) TismetSocket::kCarbon
    );
}

//===========================================================================
void ShutdownNotify::onShutdownServer(bool firstTry) {
    if (s_file)
        closeFile();
    logShutdown();
}


/****************************************************************************
*
*   Command line
*
***/

static bool recordCmd(Cli & cli);

//===========================================================================
CmdOpts::CmdOpts() {
    Cli cli;
    cli.command("record")
        .desc("Create recording of metrics received via carbon protocol.")
        .action(recordCmd);
    cli.opt(&ofile, "<output file>")
        .desc("'-' for stdout, otherwise extension defaults to '.txt'")
        .check([](auto & cli, auto & opt, auto & val) {
            if (*opt) {
                return opt->view() == "-" 
                    ? true
                    : (bool) opt->defaultExt("txt");
            } else {
                // empty path not allowed
                return cli.badUsage("Missing argument", opt.from());
            }
        });
    cli.opt(&addrStr, "[address]", "127.0.0.1:2003")
        .desc("Socket address to listen on")
        .after([](auto & cli, auto & opt, auto & val) {
            return parse(&s_opts.addr, *opt, 2003)
                || cli.badUsage(opt, *opt);
        });

    cli.group("~").title("Other");

    cli.group("When to Stop").sortKey("1");
    cli.opt(&maxBytes, "B bytes", 0)
        .desc("Max bytes to record, 0 for unlimited");
    cli.opt(&maxSecs, "S seconds", 0)
        .desc("Max seconds to record, 0 for unlimited");
    cli.opt(&maxValues, "V values", 0)
        .desc("Max values to record, 0 for unlimited");

    cli.group("Output Options").sortKey("2");
    cli.opt(&trunc, "truncate.")
        .desc("Truncate output file, if it exists.");
    cli.opt(&append, "append.")
        .desc("Append to output file, if it exists.")
        .after([&](auto & cli, auto &, auto &) {
            return (!trunc || !append)
                || cli.badUsage("Can't use both --append and --truncate.");
        });
}

//===========================================================================
static bool recordCmd(Cli & cli) {
    if (s_opts.ofile.view() != "-") {
        if (!openFile())
            return false;
    }

    shutdownMonitor(&s_cleanup);
    logStart(s_opts.ofile, s_opts.addr);
    if (s_opts.maxSecs)
        timerUpdate(&s_timer, (chrono::seconds) s_opts.maxSecs);

    carbonInitialize();
    socketListen<RecordConn>(
        s_opts.addr,
        (AppSocket::Family) TismetSocket::kCarbon
    );

    return cli.fail(EX_PENDING, "");
}
