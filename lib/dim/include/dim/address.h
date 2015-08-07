// types.h - dim core
#ifndef DIM_ADDRESS_INCLUDED
#define DIM_ADDRESS_INCLUDED

#include "dim/config.h"

struct SockAddr;


/****************************************************************************
*
*   Native
*
***/

struct sockaddr_storage;

void DimAddressToStorage (sockaddr_storage * out, const SockAddr & addr);
void DimAddressFromStorage (SockAddr * out, const sockaddr_storage & storage);


#endif
