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
*   sockaddr_storage
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


/****************************************************************************
*
*   Address query
*
***/

namespace {
    struct QueryTask : IDimTaskNotify {
        WinOverlappedEvent evt{};
        ADDRINFOEX * results{nullptr};
        IDimAddressNotify * notify{nullptr};
        HANDLE cancel{nullptr};
        int id;

        vector<SockAddr> addrs;
        WinError err{0};

        void OnTask () override;
    };
} // namespace

//===========================================================================
void QueryTask::OnTask () {
    notify->OnAddressFound(addrs.data(), (int) addrs.size());
    delete this;
}

//===========================================================================
// Variables
//===========================================================================
static int s_lastCancelId;
static unordered_map<int, QueryTask> s_tasks;

//===========================================================================
// Callback
//===========================================================================
static void CALLBACK AddressQueryCallback (
    DWORD error,
    DWORD bytes,
    OVERLAPPED * overlapped
) {
    QueryTask * task = static_cast<QueryTask *>(
        ((WinOverlappedEvent *)overlapped)->notify
    );
    task->err = error;
    if (!error && task->results) {
        ADDRINFOEX * result = task->results;
        while (result) {
            SockAddr addr;
            sockaddr_storage sas;
            memcpy(&sas, result->ai_addr, result->ai_addrlen);
            DimAddressFromStorage(&addr, sas);
            task->addrs.push_back(addr);
            result = result->ai_next;
        }
        FreeAddrInfoEx(task->results);
    }
    DimTaskPushEvent(*task);
}

//===========================================================================
// Public API
//===========================================================================
void DimAddressQuery (
    int * cancelId, 
    IDimAddressNotify * notify, 
    const std::string & name,
    int defaultPort
) {
    QueryTask * task{nullptr};
    for (;;) {
        *cancelId = ++s_lastCancelId;
        auto ib = s_tasks.try_emplace(*cancelId);
        if (ib.second) {
            task = &ib.first->second;
            break;
        }
    }
    task->evt.notify = task;
    task->id = *cancelId;
    task->notify = notify;
    WinError err = GetAddrInfoEx(
        name.c_str(),
        to_string(defaultPort).c_str(),
        NS_ALL,
        NULL,       // namespace provider id
        NULL,       // hints
        &task->results,
        NULL,       // timeout
        &task->evt.overlapped,
        &AddressQueryCallback,
        &task->cancel
    );
    if (err != ERROR_IO_PENDING)
        AddressQueryCallback(err, 0, &task->evt.overlapped);
}

//===========================================================================
void DimAddressCancelQuery (int cancelId) {
    auto it = s_tasks.find(cancelId);
    if (it != s_tasks.end())
        GetAddrInfoExCancel(&it->second.cancel);
}

//===========================================================================
void DimAddressGetLocal (std::vector<SockAddr> * out) {
    // getaddrinfo("..localmachine");
}
