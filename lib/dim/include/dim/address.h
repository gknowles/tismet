// address.h - dim services
#pragma once

#include "dim/config.h"
#include "dim/types.h"

#include <iosfwd>
#include <string>
#include <vector>

// forward declarations
struct sockaddr_storage;

namespace Dim {


/****************************************************************************
*
*   Address & Endpoint
*
***/

bool parse (Address * addr, const char src[]);
bool parse (Endpoint * end, const char src[], int defaultPort);

::std::ostream & operator<< (::std::ostream & os, const Address & addr);
::std::ostream & operator<< (::std::ostream & os, const Endpoint & end);

//===========================================================================
// Native
//===========================================================================
void copy (sockaddr_storage * out, const Endpoint & end);
void copy (Endpoint * out, const sockaddr_storage & storage);


/****************************************************************************
*
*   Lookup
*
***/

void addressGetLocal (std::vector<Address> * out);

class IEndpointNotify {
public:
    virtual ~IEndpointNotify () {}
    // count of 0 means either no results or some kind of error occurred
    virtual void onEndpointFound (Endpoint * ptr, int count) = 0;
};

void endpointQuery (
    int * cancelId, 
    IEndpointNotify * notify, 
    const std::string & name,
    int defaultPort
);
void endpointCancelQuery (int cancelId);

} // namespace dim
