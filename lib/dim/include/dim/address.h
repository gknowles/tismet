// types.h - dim core
#ifndef DIM_ADDRESS_INCLUDED
#define DIM_ADDRESS_INCLUDED

#include "dim/config.h"

#include <iosfwd>
#include <string>
#include <vector>

struct NetAddr;
struct SockAddr;


/****************************************************************************
*
*   NetAddr & SockAddr
*
***/

bool Parse (NetAddr * addr, const char src[]);
std::ostream & operator<< (std::ostream & os, const NetAddr & addr);

bool Parse (SockAddr * addr, const char src[]);
std::ostream & operator<< (std::ostream & os, const SockAddr & addr);

//===========================================================================
// Native
//===========================================================================
struct sockaddr_storage;

void DimAddressToStorage (sockaddr_storage * out, const SockAddr & addr);
void DimAddressFromStorage (SockAddr * out, const sockaddr_storage & storage);


/****************************************************************************
*
*   Address lookup
*
***/

class IDimAddressNotify {
public:
    virtual ~IDimAddressNotify () {}
    virtual void OnAddressFound (SockAddr * addr, int count) = 0;
};

void DimAddressQuery (
    int * cancelId, 
    IDimAddressNotify * notify, 
    const std::string & name,
    int defaultPort
);
void DimAddressCancelQuery (int cancelId);

void DimAddressGetLocal (std::vector<NetAddr> * out);

#endif
