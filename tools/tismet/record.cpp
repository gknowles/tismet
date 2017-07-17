// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// record.cpp - tismet
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


/****************************************************************************
*
*   RecordConn
*
***/

namespace {
class RecordConn : public ICarbonNotify {
    string_view m_name;
    string m_buf;
public:
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(uint32_t id, float value, TimePoint time) override;
};
} // namespace

//===========================================================================
uint32_t RecordConn::onCarbonMetric(string_view name) {
    m_name = name;
    return 1;
}

//===========================================================================
void RecordConn::onCarbonValue(uint32_t id, float value, TimePoint time) {
    assert(id == 1);
    m_buf.clear();
    carbonWrite(m_buf, m_name, value, time);
    fileAppendWait(s_file, m_buf.data(), m_buf.size());
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

//===========================================================================
static bool recordCmd(Cli & cli) {
    shutdownMonitor(&s_cleanup);

    if (!s_out)
        return cli.badUsage("No value given for <output file[.txt]>");
    if (s_out->view() != "-") {
        s_file = fileOpen(
            s_out->defaultExt("txt").view(), 
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
        return cli.badUsage(*s_endptOpt, "Invalid endpoint");

    logMsgDebug() << "Recording to " << *s_out;
    logMsgDebug() << "Control-C to stop recording";
    consoleEnableCtrlC();
    
    carbonInitialize();
    socketListen<RecordConn>(
        s_endpt,
        (AppSocket::Family) TismetSocket::kCarbon
    );

    return true;
}
