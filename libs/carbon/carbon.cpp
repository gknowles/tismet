// Copyright Glen Knowles 2017 - 2018.
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

const unsigned kCarbonMaxRecordSize = 1024;


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
bool ICarbonNotify::append(string_view src) {
    CarbonUpdate upd;
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
            return true;
        }
        s_perfUpdates += 1;
        auto id = onCarbonMetric(upd.name);
        onCarbonValue(id, upd.time, upd.value);
    }

    s_perfErrors += 1;
    return false;
}


/****************************************************************************
*
*   ICarbonSocketNotify
*
***/

//===========================================================================
bool ICarbonSocketNotify::onSocketAccept(const AppSocketInfo & info) {
    s_perfClients += 1;
    s_perfCurrent += 1;
    return true;
}

//===========================================================================
void ICarbonSocketNotify::onSocketDisconnect() {
    s_perfCurrent -= 1;
}

//===========================================================================
void ICarbonSocketNotify::onSocketRead(AppSocketData & data) {
    if (!append(string_view(data.data, data.bytes))) {
        s_perfErrors += 1;
        socketDisconnect(this);
    }
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
    CarbonUpdate upd;
    if (!carbonParse(upd, view))
        return AppSocket::kUnsupported;
    if (upd.name.empty()) {
        if (view.size() < kCarbonMaxRecordSize) {
            return AppSocket::kUnknown;
        } else {
            return AppSocket::kUnsupported;
        }
    }

    return AppSocket::kPreferred;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void carbonInitialize() {
    socketAddFamily((AppSocket::Family) TismetSocket::kCarbon, &s_sockMatch);
}

//===========================================================================
// characters allowed in metric names:
//  graphite:
//      normal: alnum + !#$%&"'+-.:;<=>?@^_`~\
//      strict: a-z A-Z _ - . =
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
    if (src.empty())
        return true;
    const char * ptr = src.data();
    CarbonParser parser(&upd);
    parser.parse(ptr);
    auto pos = parser.errpos();
    if (!upd.name.empty()) {
        src.remove_prefix(pos + 1);
        return true;
    }
    return !ptr[pos];
}

//===========================================================================
void carbonWrite(
    ostream & os,
    string_view name,
    TimePoint time,
    double value
) {
    StrFrom<double> vstr(value);
    StrFrom<time_t> tstr(Clock::to_time_t(time));
    os << name << ' ' << vstr << ' ' << tstr << '\n';
}

//===========================================================================
void carbonWrite(
    string & out,
    string_view name,
    TimePoint time,
    double value
) {
    StrFrom<double> vstr(value);
    StrFrom<time_t> tstr(Clock::to_time_t(time));
    out += name;
    out += ' ';
    out += vstr;
    out += ' ';
    out += tstr;
    out += '\n';
}
