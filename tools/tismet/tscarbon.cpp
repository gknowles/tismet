// Copyright Glen Knowles 2017.
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
static TsdFileHandle s_tsd;


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
    // Inherited via ICarbonSocketNotify
    uint32_t onCarbonMetric(string_view name) override;
    void onCarbonValue(uint32_t id, TimePoint time, float value) override;
};

} // namespace

//===========================================================================
uint32_t RecordConn::onCarbonMetric(string_view name) {
    uint32_t id;
    tsdInsertMetric(id, s_tsd, name);
    return id;
}

//===========================================================================
void RecordConn::onCarbonValue(uint32_t id, TimePoint time, float value) {
    assert(id);
    tsdUpdateValue(s_tsd, id, time, value);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsCarbonInitialize() {
    s_tsd = tsDataHandle();
    carbonInitialize();
    s_mgr = sockMgrListen(
        "carbon",
        getFactory<IAppSocketNotify, RecordConn>(),
        (AppSocket::Family) TismetSocket::kCarbon
    );
}
