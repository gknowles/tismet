// winaddress.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

#pragma warning(disable: 4996) // deprecated


/****************************************************************************
*
*   SockAddr
*
***/

//===========================================================================
bool Parse (SockAddr * addr, const char src[]) {
    sockaddr_storage sas;
    int sasLen = sizeof(sas);
    if (SOCKET_ERROR == WSAStringToAddress(
        (char *) src,
        AF_INET,
        NULL,
        (sockaddr *) &sas,
        &sasLen
    )) {
        *addr = {};
        return false;
    }
    DimAddressFromStorage(addr, sas);
    return true;
}

//===========================================================================
std::ostream & operator<< (std::ostream & os, const SockAddr & addr) {
    sockaddr_storage sas;
    DimAddressToStorage(&sas, addr);
    char tmp[256];
    DWORD tmpLen = sizeof(tmp);
    if (SOCKET_ERROR == WSAAddressToString(
        (sockaddr *) &sas,
        sizeof(sas),
        NULL,
        tmp,
        &tmpLen
    )) {
        os << "(bad_sockaddr)";
    } else {
        os << tmp;
    }
    return os;
}


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
