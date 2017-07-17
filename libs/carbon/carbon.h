// Copyright Glen Knowles 2017.
// Distributed under the Boost Software License, Version 1.0.
//
// carbon.h - tismet carbon
//
// Implemetation of graphite's carbon protocol for receiving metric data
#pragma once

#include "net/net.h"

#include <string_view>


/****************************************************************************
*
*   Declarations
*
***/

namespace TismetSocket {
    enum Family {
        kCarbon = Dim::AppSocket::Family::kNumFamilies,
    };
}


/****************************************************************************
*
*   Implemented by clients
*
***/

class ICarbonNotify : public Dim::IAppSocketNotify {
public:
    virtual uint32_t onCarbonMetric(std::string_view name) = 0;
    virtual void onCarbonValue(
        uint32_t id,
        float value,
        Dim::TimePoint time
    ) = 0;

private:
    // Inherited via IAppSocketNotify
    bool onSocketAccept(const Dim::AppSocketInfo & info) override;
    void onSocketDisconnect() override;
    void onSocketRead(Dim::AppSocketData & data) override;

    std::string m_buf;
};


/****************************************************************************
*
*   Public API
*
***/

void carbonInitialize ();


struct CarbonUpdate {
    std::string_view name;
    float value;
    Dim::TimePoint time;
};

// Returns false on malformed input, and true otherwise (update successful
// parsed or more data needed). On return if upd.name is empty more data is 
// needed and src is unchanged, otherwise upd is fully populated and src is
// adjusted to reference the leftover suffix of src that was not parsed.
bool carbonParse(CarbonUpdate & upd, std::string_view & src);

void carbonWrite(
    std::string & out, 
    std::string_view name, 
    float value, 
    Dim::TimePoint time
);
