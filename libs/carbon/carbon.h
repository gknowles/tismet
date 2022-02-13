// Copyright Glen Knowles 2017 - 2021.
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

class ICarbonNotify {
public:
    static void ackValue(unsigned reqId, unsigned completed);

public:
    virtual ~ICarbonNotify();

    //-----------------------------------------------------------------------
    // For consumers

    // Returns false for each value that has its processing delayed. All
    // delayed values must be accounted for after they have completed by calls
    // to carbonAckValue.
    virtual bool onCarbonValue(
        unsigned reqId,
        std::string_view name,
        Dim::TimePoint time,
        double value,
        uint32_t idHint = 0
    ) = 0;

    //-----------------------------------------------------------------------
    // For producers (ICarbonSocketNotify and ICarbonFileNotify)

    // Clears state of all incomplete requests
    void clear();

    // Returns number of onCarbonValue() calls that requested delayed reads,
    // or EOF on malformed data. When EOF is returned, there may have been any
    // number of onCarbonValue calls before the error was detected.
    unsigned append(std::string_view data);

    // Called when an append request is completed, either synchronously or
    // asynchronously via carbonAckValue.
    virtual void onCarbonRequestComplete() {}

private:
    std::string m_buf;
    Dim::UnsignedSet m_requestIds;
};

class ICarbonSocketNotify
    : public Dim::IAppSocketNotify
    , public ICarbonNotify
{
private:
    // Inherited via IAppSocketNotify
    bool onSocketAccept(const Dim::AppSocketInfo & info) override;
    void onSocketDisconnect() override;
    bool onSocketRead(Dim::AppSocketData & data) override;

    // Inherited via ICarbonNotify
    void onCarbonRequestComplete() override;
};

class ICarbonFileNotify
    : public Dim::IFileReadNotify
    , public ICarbonNotify
{
protected:
    // Inherited via IFileReadNotify
    bool onFileRead(
        size_t * bytesUsed, 
        const Dim::FileReadData & data
    ) override;
};


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
// Listening for carbon protocol connections
//===========================================================================
void carbonInitialize ();

// Called as onCarbonValue calls that returned false are completed. There may
// be multiple carbon values with the same request id, and all of their
// completions must be acknowledged.
void carbonAckValue (unsigned reqId, unsigned completed);


//===========================================================================
// Basic building/parsing
//===========================================================================
struct CarbonUpdate {
    std::string_view name;
    double value{};
    Dim::TimePoint time;
};

// Returns false on malformed input, and true otherwise (update successfully
// parsed or more data needed). On return if upd.name is empty more data is
// needed and src is unchanged, otherwise upd is fully populated and src is
// adjusted to reference the leftover suffix of src that was not parsed.
bool carbonParse(
    CarbonUpdate & upd,
    std::string_view & src,
    Dim::TimePoint now
);

void carbonWrite(
    std::ostream & os,
    std::string_view name,
    Dim::TimePoint time,
    double value
);
void carbonWrite(
    std::string & out,
    std::string_view name,
    Dim::TimePoint time,
    double value
);
