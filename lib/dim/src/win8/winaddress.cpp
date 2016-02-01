// winaddress.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

#pragma warning(disable: 4996) // deprecated


/****************************************************************************
*
*   Endpoint
*
***/

//===========================================================================
bool Parse (Address * out, const char src[]) {
    Endpoint sa;
    if (!Parse(&sa, src, 9)) {
        *out = {};
        return false;
    }
    *out = sa.addr;
    return true;
}

//===========================================================================
std::ostream & operator<< (std::ostream & os, const Address & addr) {
    Endpoint sa;
    sa.addr = addr;
    return operator<<(os, sa);
}


/****************************************************************************
*
*   Endpoint
*
***/

//===========================================================================
bool Parse (Endpoint * end, const char src[], int defaultPort) {
    sockaddr_storage sas;
    int sasLen = sizeof(sas);
    if (SOCKET_ERROR == WSAStringToAddress(
        (char *) src,
        AF_INET,
        NULL,
        (sockaddr *) &sas,
        &sasLen
    )) {
        *end = {};
        return false;
    }
    DimEndpointFromStorage(end, sas);
    if (!end->port)
        end->port = defaultPort;
    return true;
}

//===========================================================================
std::ostream & operator<< (std::ostream & os, const Endpoint & src) {
    sockaddr_storage sas;
    DimEndpointToStorage(&sas, src);
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
void DimEndpointToStorage (sockaddr_storage * out, const Endpoint & src) {
    *out = {};
    auto ia = reinterpret_cast<sockaddr_in*>(out);
    ia->sin_family = AF_INET;
    ia->sin_port = htons((short) src.port);
    ia->sin_addr.s_addr = htonl(src.addr.data[3]);
}

//===========================================================================
void DimEndpointFromStorage (
    Endpoint * out, 
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
        ADDRINFOEXW * results{nullptr};
        IDimEndpointNotify * notify{nullptr};
        HANDLE cancel{nullptr};
        int id;

        vector<Endpoint> ends;
        WinError err{0};

        void OnTask () override;
    };
} // namespace

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
    if (task->results) {
        ADDRINFOEXW * result = task->results;
        while (result) {
            if (result->ai_family == AF_INET) {
                Endpoint end;
                sockaddr_storage sas{};
                memcpy(&sas, result->ai_addr, result->ai_addrlen);
                DimEndpointFromStorage(&end, sas);
                task->ends.push_back(end);
            }
            result = result->ai_next;
        }
        FreeAddrInfoExW(task->results);
    }
    DimTaskPushEvent(*task);
}

//===========================================================================
// QueryTask
//===========================================================================
void QueryTask::OnTask () {
    if (err && err != WSA_E_CANCELLED) {
        DimLog{kError} << "GetAddrInfoEx: " << err;
    }
    notify->OnEndpointFound(ends.data(), (int) ends.size());
    s_tasks.erase(id);
}

//===========================================================================
// Public API
//===========================================================================
void DimEndpointQuery (
    int * cancelId, 
    IDimEndpointNotify * notify, 
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

    // if the name is the string form of an address just return the address
    Endpoint end;
    if (Parse(&end, name.c_str(), defaultPort)) {
        task->ends.push_back(end);
        DimTaskPushEvent(*task);
        return;
    }

    // Async completion requires wchar version of 
    wstring wname;
    wstring wport{to_wstring(defaultPort)};
    wname.resize(name.size() + 1);
    int chars = MultiByteToWideChar(
        CP_UTF8,
        0,
        name.data(),
        (int) name.size(),
        (wchar_t *) wname.data(),
        (int) wname.size()
    );
    wname.resize(chars);

    // extract non-default port if present in name
    size_t pos = wname.rfind(L':');
    if (pos != string::npos) {
        wport = wname.substr(pos + 1);
        wname.resize(pos);
    }

    WinError err = GetAddrInfoExW(
        wname.c_str(),
        wport.c_str(),
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
void DimEndpointCancelQuery (int cancelId) {
    auto it = s_tasks.find(cancelId);
    if (it != s_tasks.end())
        GetAddrInfoExCancel(&it->second.cancel);
}

//===========================================================================
void DimAddressGetLocal (std::vector<Address> * out) {
    out->resize(0);
    ADDRINFO * result;
    WinError err = getaddrinfo(
        "..localmachine",
        NULL,   // service name
        NULL,   // hints
        &result
    );
    if (err)
        DimLog{kCrash} << "getaddrinfo(..localmachine): " << err;

    Endpoint end;
    sockaddr_storage sas;
    while (result) {
        if (result->ai_family == AF_INET) {
            memcpy(&sas, result->ai_addr, result->ai_addrlen);
            DimEndpointFromStorage(&end, sas); 
            out->push_back(end.addr);
        }
        result = result->ai_next;
    }

    // if there are no addresses toss on the loopback so we can at least
    // pretend.
    if (out->empty()) {
        Parse(&end, "127.0.0.1", 9);
        out->push_back(end.addr);
    }
}