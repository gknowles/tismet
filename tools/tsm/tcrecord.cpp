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
*   Variables
*
***/

static FileHandle s_file;
static Endpoint s_endpt;
static uint64_t s_maxBytes;
static uint64_t s_maxSecs;
static uint64_t s_valuesWritten;
static uint64_t s_bytesWritten;

static TimePoint s_startTime;


//===========================================================================
static void logStart(string_view target, const Endpoint & source) {
    s_startTime = Clock::now();
    logMsgInfo() << "Recording " << source << " into " << target;
    if (s_maxBytes || s_maxSecs) {
        auto os = logMsgInfo();
        os.imbue(locale(""));
        os << "Limits";
        if (s_maxBytes) 
            os << "; bytes: " << s_maxBytes;
        if (s_maxSecs)
            os << "; seconds: " << s_maxSecs;
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
*   RecordConn
*
***/

namespace {

class RecordConn : public ICarbonSocketNotify {
    string_view m_name;
    string m_buf;
public:
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(uint32_t id, TimePoint time, double value) override;
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
    fileAppendWait(s_file, m_buf.data(), m_buf.size());
    s_bytesWritten += m_buf.size();
    s_valuesWritten += 1;
    if (s_bytesWritten >= s_maxBytes && !appStopping()) {
        logMsgInfo() << "Maximum bytes received";
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
    void onShutdownClient(bool firstTry) override;
};

} // namespace

static ShutdownNotify s_cleanup;

//===========================================================================
void ShutdownNotify::onShutdownClient(bool firstTry) {
    socketCloseWait<RecordConn>(
        s_endpt,
        (AppSocket::Family) TismetSocket::kCarbon
    );
    fileClose(s_file);
    logShutdown();
}


/****************************************************************************
*
*   Command line
*
***/

static bool recordCmd(Cli & cli);

static Cli s_cli = Cli{}.command("record")
    .desc("Create recording of metrics received via carbon protocol.")
    .action(recordCmd);
static auto & s_out = s_cli.opt<Path>("<output file>", "")
    .desc("'-' for stdout, otherwise extension defaults to '.txt'");
static auto & s_endptOpt = s_cli.opt<string>("[endpoint]", "127.0.0.1:2003")
    .desc("Endpoint to listen on");
static auto & s_bytesOpt = s_cli.opt(&s_maxBytes, "b bytes", 0)
    .desc("Bytes to record, 0 for unlimited");
static auto & s_secsOpt = s_cli.opt(&s_maxSecs, "s seconds", 0)
    .desc("Seconds to record, 0 for unlimited");

//===========================================================================
static bool recordCmd(Cli & cli) {
    shutdownMonitor(&s_cleanup);

    if (!s_out)
        return cli.badUsage("No value given for <output file[.txt]>");
    if (s_out->str() != "-") {
        s_file = fileOpen(
            s_out->defaultExt("txt"), 
            File::fReadWrite | File::fCreat | File::fTrunc | File::fBlocking
        );
        if (!s_file) {
            return cli.fail(
                EX_DATAERR, 
                string(*s_out) + ": open <outputFile[.txt]> failed"
            );
        }
    }

    if (!parse(&s_endpt, *s_endptOpt, 2003))
        return cli.badUsage("Bad '" + s_endptOpt.from() + "' endpoint");

    logStart(*s_out, s_endpt);
    if (s_maxSecs)
        timerUpdate(&s_timer, (chrono::seconds) s_maxSecs);

    carbonInitialize();
    socketListen<RecordConn>(
        s_endpt,
        (AppSocket::Family) TismetSocket::kCarbon
    );

    return cli.fail(EX_PENDING, "");
}
