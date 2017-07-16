// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// carbon.cpp - tismet carbon
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Tuning parameters
*
***/


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfClients = uperf("carbon clients");
static auto & s_perfCurrent = uperf("carbon clients (current)");
static auto & s_perfUpdates = uperf("carbon updates");
static auto & s_perfErrors = uperf("carbon errors"); 


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   ICarbonNotify
*
***/

//===========================================================================
bool ICarbonNotify::onSocketAccept(const AppSocketInfo & info) {
    s_perfClients += 1;
    s_perfCurrent += 1;
    return true;
}

//===========================================================================
void ICarbonNotify::onSocketDisconnect() {
    s_perfCurrent -= 1;
}

//===========================================================================
void ICarbonNotify::onSocketRead(AppSocketData & data) {
    CarbonUpdate upd;
    auto src = string_view(data.data, data.bytes);
    if (!m_buf.empty()) {
        m_buf.append(src);
        src = m_buf;
    }
    while (carbonParse(upd, src)) {
        if (upd.name.empty()) {
            if (m_buf.empty()) {
                m_buf = src;
            } else {
                m_buf.erase(0, m_buf.size() - src.size());
            }
            return;
        }
        s_perfUpdates += 1;
        auto id = onCarbonMetric(upd.name);
        onCarbonValue(id, upd.value, upd.time);
    }

    s_perfErrors += 1;
    return socketDisconnect(this);
}


/****************************************************************************
*
*   CarbonMatch
*
***/

namespace {
class CarbonMatch : public IAppSocketMatchNotify {
    AppSocket::MatchType OnMatch(
        AppSocket::Family fam, 
        string_view view) override;
};
static CarbonMatch s_sockMatch;
} // namespace

//===========================================================================
AppSocket::MatchType CarbonMatch::OnMatch(
    AppSocket::Family fam,
    string_view view
) {
    assert(fam == TismetSocket::kCarbon);
    return AppSocket::kUnsupported;
//    return AppSocket::kPreferred;

  //  return AppSocket::kUnknown;
}


/****************************************************************************
*
*   Shutdown monitor
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
    //if (firstTry && !s_paths.empty())
    //    socketCloseWait<HttpSocket>(s_endpoint, AppSocket::kHttp2);
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void carbonInitialize() {
    shutdownMonitor(&s_cleanup);
    socketAddFamily((AppSocket::Family) TismetSocket::kCarbon, &s_sockMatch);
}

//===========================================================================
// characters allowed in metric names:
//  graphite: 
//      normal: alnum + !#$%&"'+-.:;<=>?@^_`|~\
//      wildcards: *[]{}
//  OpenTSDB:
//      alnum + -_./
//  InfluxDB:
//      allowed: all
//      recommended to avoid: non-printable + \^$'"=,
//  prometheus:
//      [a-zA-Z_:]([a-zA-Z0-9+:])*
bool carbonParse(CarbonUpdate & upd, string_view & src) {
    assert(*src.end() == 0);
    upd.name = {};
    const char * ptr = src.data();
    CarbonParser parser(&upd);
    parser.parse(ptr);
    auto pos = parser.errpos();
    if (!upd.name.empty()) {
        src.remove_prefix(pos);
        return true;
    }
    return ptr[pos];
}

//===========================================================================
void carbonWrite(string & out, string_view name, float value, TimePoint time) {
    out += name;
    out += ' ';
    out += to_string(value);
    out += ' ';
    out += to_string(Clock::to_time_t(time));
    out += '\n';
}
