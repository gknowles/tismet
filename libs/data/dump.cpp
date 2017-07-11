// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// dump.cpp - tismet
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

const char kDumpVersion[] = "Tismet Dump Version 2017.1";

const unsigned kMaxMetricNameLen = 64;
static_assert(kMaxMetricNameLen <= numeric_limits<unsigned char>::max());

namespace {

class Writer : public ITsdEnumNotify {
public:
    Writer(ostream & os);

    bool OnTsdValue(
        uint32_t id, 
        string_view name, 
        TimePoint time, 
        float val
    ) override;

private:
    ostream & m_os;
};

} // namespace


/****************************************************************************
*
*   Writer
*
***/

//===========================================================================
Writer::Writer(ostream & os) 
    : m_os{os}
{}

//===========================================================================
bool Writer::OnTsdValue(
    uint32_t id, 
    string_view name, 
    TimePoint time, 
    float val
) {
    m_os << name << ' ' << val << ' ' << Clock::to_time_t(time) << '\n';
    return true;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void tsdWriteDump(std::ostream & os, TsdFileHandle h, string_view wildname) {
    UnsignedSet ids;
    tsdFindMetrics(ids, h, wildname);
    os << kDumpVersion << '\n';
    Writer out(os);
    for (auto && id : ids) {
        tsdEnumValues(&out, h, id);
    }
}
