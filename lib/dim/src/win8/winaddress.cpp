// winaddress.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void DimAddressToStorage (sockaddr_storage * out, const SockAddr & addr) {
    *out = {};
    auto ia = reinterpret_cast<sockaddr_in*>(out);
    ia->sin_family = AF_INET;
    ia->sin_port = htons((short) addr.port);
    ia->sin_addr.s_addr = htonl(addr.addr.data[3]);
}

//===========================================================================
void DimAddressFromStorage (
    SockAddr * out, 
    const sockaddr_storage & storage
) {
    *out = {};
    auto ia = reinterpret_cast<const sockaddr_in&>(storage);
    assert(ia.sin_family == AF_INET);
    out->port = ntohs(ia.sin_port);
    out->addr.data[3] = ntohl(ia.sin_addr.s_addr);
}
