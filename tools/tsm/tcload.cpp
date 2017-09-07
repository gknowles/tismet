// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// tcload.cpp - tsm
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*     
*   Command line
*     
***/

static bool loadCmd(Cli & cli);

static Cli s_cli = Cli{}.command("load")
    .desc("Load metrics from dump file into database")
    .action(loadCmd);
static auto & s_dat = s_cli.opt<Path>("[dat file]")
    .desc("Target database");
static auto & s_in = s_cli.opt<Path>("[input file]")
    .desc("File to load (default extension: .txt)");


/****************************************************************************
*     
*   LoadProgress
*     
***/

namespace {

class LoadProgress : public ITsdProgressNotify {
public:
    LoadProgress(TsdFileHandle h) : m_h{h} {}

    // Inherited via ITsdProgressNotify
    bool OnTsdProgress(bool complete, const TsdProgressInfo & info) override;

private:
    TsdFileHandle m_h;
};

} // namespace

//===========================================================================
bool LoadProgress::OnTsdProgress(
    bool complete, 
    const TsdProgressInfo & info
) {
    if (complete) {
        logMsgInfo() << "Loaded: " 
            << info.bytes << " bytes, "
            << info.values << " values";
        tsdClose(m_h);
        appSignalShutdown(EX_OK);
        delete this;
    }
    return true;
}


/****************************************************************************
*     
*   Load command
*     
***/

//===========================================================================
static bool loadCmd(Cli & cli) {
    if (!s_dat)
        return cli.badUsage("No value given for <dat file[.dat]>");

    auto h = tsdOpen(s_dat->defaultExt("dat"));
    auto progress = make_unique<LoadProgress>(h);
    tsdLoadDump(progress.release(), h, s_in->defaultExt("txt"));
    return true;
}
