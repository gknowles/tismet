// Copyright Glen Knowles 2017 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//
// tscarbon.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Variables
*
***/

static SockMgrHandle s_mgr;


/****************************************************************************
*
*   RecordConn
*
***/

namespace {

class RecordConn : public ICarbonSocketNotify {
    string m_buf;
public:
    // Inherited via ICarbonSocketNotify
    void onCarbonValue(
        string_view name,
        TimePoint time,
        double value,
        uint32_t idHint
    ) override;
};

} // namespace

//===========================================================================
void RecordConn::onCarbonValue(
    string_view name,
    TimePoint time,
    double value,
    uint32_t id
) {
    auto ctx = tsDataOpenContext();
    tsDataInsertMetric(&id, ctx, name);
    dbUpdateSample(ctx, id, time, value);
    dbCloseContext(ctx);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsCarbonInitialize() {
    carbonInitialize();
    s_mgr = sockMgrListen(
        "carbon",
        getFactory<IAppSocketNotify, RecordConn>(),
        (AppSocket::Family) TismetSocket::kCarbon
    );
}
