// types.h - dim services
#ifndef DIM_ADDRESS_INCLUDED
#define DIM_ADDRESS_INCLUDED

#include "dim/config.h"

#include <iosfwd>
#include <string>
#include <vector>

struct Address;
struct Endpoint;


/****************************************************************************
*
*   Address & Endpoint
*
***/

bool Parse (Address * addr, const char src[]);
std::ostream & operator<< (std::ostream & os, const Address & addr);

bool Parse (Endpoint * end, const char src[], int defaultPort);
std::ostream & operator<< (std::ostream & os, const Endpoint & end);

//===========================================================================
// Native
//===========================================================================
struct sockaddr_storage;

void DimEndpointToStorage (sockaddr_storage * out, const Endpoint & end);
void DimEndpointFromStorage (Endpoint * out, const sockaddr_storage & storage);


/****************************************************************************
*
*   Lookup
*
***/

void DimAddressGetLocal (std::vector<Address> * out);

class IDimEndpointNotify {
public:
    virtual ~IDimEndpointNotify () {}
    virtual void OnEndpointFound (Endpoint * ptr, int count) = 0;
};

void DimEndpointQuery (
    int * cancelId, 
    IDimEndpointNotify * notify, 
    const std::string & name,
    int defaultPort
);
void DimEndpointCancelQuery (int cancelId);

#endif
