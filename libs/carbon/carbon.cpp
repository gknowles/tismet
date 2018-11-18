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

unsigned const kCarbonMaxRecordSize = 1024;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct IncompleteRequest {
    ICarbonSocketNotify * socket;
    unsigned incomplete;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static auto & s_perfClients = uperf("carbon.clients");
static auto & s_perfCurrent = uperf("carbon.clients (current)");
static auto & s_perfUpdates = uperf("carbon.updates");
static auto & s_perfErrors = uperf("carbon.errors");

static unordered_map<unsigned, IncompleteRequest> s_incompletes;
static unsigned s_nextRequestId;


/****************************************************************************
*
*   ICarbonNotify
*
***/

//===========================================================================
unsigned ICarbonNotify::append(unsigned reqId, string_view src) {
    auto now = timeNow();
    CarbonUpdate upd;
    if (!m_buf.empty()) {
        m_buf.append(src);
        src = m_buf;
    }
    unsigned incomplete = 0;
    while (carbonParse(upd, src, now)) {
        if (upd.name.empty()) {
            if (m_buf.empty()) {
                m_buf = src;
            } else {
                m_buf.erase(0, m_buf.size() - src.size());
            }
            return incomplete;
        }
        s_perfUpdates += 1;
        if (!onCarbonValue(reqId, upd.name, upd.time, upd.value))
            incomplete += 1;
    }

    s_perfErrors += 1;
    return (unsigned) EOF;
}


/****************************************************************************
*
*   ICarbonSocketNotify
*
***/

//===========================================================================
bool ICarbonSocketNotify::onSocketAccept(AppSocketInfo const & info) {
    s_perfClients += 1;
    s_perfCurrent += 1;
    socketWrite(this, "SERVER = tismet/1.0\n");
    return true;
}

//===========================================================================
void ICarbonSocketNotify::onSocketDisconnect() {
    s_perfCurrent -= 1;
    for (auto && id : m_requestIds)
        s_incompletes.erase(id);
}

//===========================================================================
static unsigned nextRequestId() {
    for (;;) {
        auto id = ++s_nextRequestId;
        if (id && !s_incompletes.count(id))
            return id;
    }
}

//===========================================================================
bool ICarbonSocketNotify::onSocketRead(AppSocketData & data) {
    auto id = nextRequestId();
    auto incomplete = append(id, string_view(data.data, data.bytes));
    if (incomplete == EOF) {
        socketDisconnect(this);
    } else if (incomplete) {
        m_requestIds.insert(id);
        s_incompletes[id] = {this, incomplete};
        return false;
    }
    return true;
}

//===========================================================================
// static
void ICarbonSocketNotify::ackValue(unsigned reqId, unsigned completed) {
    assert(reqId && completed);
    auto i = s_incompletes.find(reqId);
    if (i == s_incompletes.end())
        return;
    auto & incomplete = i->second;
    if (incomplete.incomplete < completed)
        logMsgFatal() << "too many carbon value acknowledgments";
    if (incomplete.incomplete -= completed)
        return;
    auto sock = incomplete.socket;
    sock->m_requestIds.erase(reqId);
    socketRead(sock);
    s_incompletes.erase(i);
}


/****************************************************************************
*
*   CarbonMatch
*
***/

namespace {

class CarbonMatch : public IAppSocketMatchNotify {
    AppSocket::MatchType onMatch(
        AppSocket::Family fam,
        string_view view
    ) override;
};
static CarbonMatch s_sockMatch;

} // namespace

//===========================================================================
AppSocket::MatchType CarbonMatch::onMatch(
    AppSocket::Family fam,
    string_view view
) {
    assert(fam == TismetSocket::kCarbon);
    CarbonUpdate upd;
    if (!carbonParse(upd, view, {}))
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
void carbonAckValue(unsigned reqId, unsigned completed) {
    ICarbonSocketNotify::ackValue(reqId, completed);
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
bool carbonParse(CarbonUpdate & upd, string_view & src, TimePoint now) {
    assert(*src.end() == 0);
    upd.name = {};
    if (src.empty())
        return true;
    char const * ptr = src.data();
    CarbonParser parser(&upd);
    parser.parse(ptr);
    auto pos = parser.errpos();
    if (!upd.name.empty()) {
        src.remove_prefix(pos + 1);
        if (!upd.time)
            upd.time = now;
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
    StrFrom<time_t> tstr(timeToUnix(time));
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
    StrFrom<time_t> tstr(timeToUnix(time));
    out += name;
    out += ' ';
    out += vstr;
    out += ' ';
    out += tstr;
    out += '\n';
}
