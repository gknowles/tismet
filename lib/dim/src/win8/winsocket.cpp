// socket.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Incomplete public types
*
***/

class DimSocket {
public:
    static void Connect (
        IDimSocketNotify * notify,
        const SockAddr & remoteAddr,
        const SockAddr & localAddr
    );
};


/****************************************************************************
*
*   Private declarations
*
***/

namespace {

struct RegisteredBuffer;
struct Buffer {
    union {
        int ownerPos;
        int nextPos;
    };
};
struct RegisteredBuffer {
    RIO_BUFFERID id;
    Buffer * base;
    TimePoint lastUsed;
    int bufferSize;
    int reserved;
    int size;
    int used;
    int firstFree;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static RIO_EXTENSION_FUNCTION_TABLE s_rio;

static int s_bufferSize{4096};
static size_t s_registeredBufferSize{256 * 4096};
static size_t s_minLargePage;
static size_t s_minPage;

// buffers are kept sorted by state (full, partial, empty)
static vector<RegisteredBuffer> s_rbuffers;
static int s_numPartial;
static int s_numFull;

static ERunMode s_mode;


/****************************************************************************
*
*   Helpers
*
***/


/****************************************************************************
*
*   RegisteredBuffer
*
***/

//===========================================================================
static void CreateEmptyBuffer () {
    size_t bytes = s_registeredBufferSize;
    size_t granularity = s_minPage;
    if (s_minLargePage && s_registeredBufferSize >= s_minLargePage) {
        granularity = s_minLargePage;
    }
    // round up, but not to exceed DWORD
    bytes += granularity - 1;
    bytes = bytes - bytes % granularity;
    if (bytes > numeric_limits<DWORD>::max()) {
        bytes = numeric_limits<DWORD>::max() / granularity * granularity;
    }

    s_rbuffers.emplace_back();
    RegisteredBuffer & rbuf = s_rbuffers.back();
    rbuf.bufferSize = 4096;
    rbuf.reserved = int(bytes / 4096);
    rbuf.lastUsed = TimePoint::min();
    rbuf.size = 0;
    rbuf.used = 0;
    rbuf.firstFree = 0;
    rbuf.base = (Buffer *) VirtualAlloc(
        nullptr,
        bytes,
        MEM_COMMIT 
            | MEM_RESERVE 
            | (bytes > s_minLargePage ? MEM_LARGE_PAGES : 0),
        PAGE_READWRITE
    );

    rbuf.id = s_rio.RIORegisterBuffer(
        (char *) rbuf.base, 
        (DWORD) bytes
    );
    if (rbuf.id == RIO_INVALID_BUFFERID) {
        DimErrorLog{kError} << "RIORegisterBuffer failed, " 
            << WSAGetLastError();
    }
}

//===========================================================================
static void DestroyEmptyBuffer () {
    assert(!s_rbuffers.empty());
    RegisteredBuffer & rbuf = s_rbuffers.back();
    assert(!rbuf.used);
    s_rio.RIODeregisterBuffer(rbuf.id);
    VirtualFree(rbuf.base, 0, MEM_RELEASE);
    s_rbuffers.pop_back();
}


/****************************************************************************
*
*   DimSocket
*
***/

//===========================================================================
// static
void DimSocket::Connect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    if (!notify->m_socket) {
        
    }

    ref(notify);
    ref(remoteAddr);
    ref(localAddr);
}


/****************************************************************************
*
*   DimSocketShutdown
*
***/

namespace {
class DimSocketShutdown : public IDimAppShutdownNotify {
    bool OnAppQueryConsoleDestroy () override;
};
static DimSocketShutdown s_cleanup;
} // namespace

//===========================================================================
bool DimSocketShutdown::OnAppQueryConsoleDestroy () {
    s_mode = MODE_STOPPING;
    if (WSACleanup()) 
        DimErrorLog{kError} << "WSACleanup failed, " << WSAGetLastError();
    s_mode = MODE_STOPPED;
    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void IDimSocketInitialize () {
    s_mode = MODE_STARTING;
    DimAppMonitorShutdown(&s_cleanup);
    WSADATA data = {};
    int error = WSAStartup(WINSOCK_VERSION, &data);
    if (error || data.wVersion != WINSOCK_VERSION) {
        DimErrorLog{kFatal} << "WSAStartup failed, " << error
            << "version " << hex << data.wVersion;
    }

    SOCKET s = WSASocketW(
        AF_UNSPEC, 
        SOCK_STREAM, 
        IPPROTO_TCP,
        NULL,   // protocol info (additional creation options)
        0,      // socket group
        WSA_FLAG_REGISTERED_IO
    );
    if (s == INVALID_SOCKET) 
        DimErrorLog{kFatal} << "socket failed, " << WSAGetLastError();

    // get RIO functions
    GUID extId = WSAID_MULTIPLE_RIO;
    s_rio.cbSize = sizeof(s_rio);
    DWORD bytes;
    if (WSAIoctl(
        s,
        SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
        &extId, sizeof(extId),
        &s_rio, sizeof(s_rio),
        &bytes,
        NULL,
        NULL
    )) {
        DimErrorLog{kFatal} << "WSAIoctl get RIO extension failed, " 
            << WSAGetLastError();
    }

    s_minLargePage = GetLargePageMinimum();
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    s_minPage = info.dwAllocationGranularity;

    // if large pages are available make sure the buffers are at least 
    // that big
    s_registeredBufferSize = max(s_minLargePage, s_registeredBufferSize);

    closesocket(s);
    s_mode = MODE_RUNNING;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
void DimSocketConnect (
    IDimSocketNotify * notify,
    const SockAddr & remoteAddr,
    const SockAddr & localAddr
) {
    DimSocket::Connect(notify, remoteAddr, localAddr);
}

//===========================================================================
void DimSocketDisconnect (IDimSocketNotify * notify);

//===========================================================================
void DimSocketWrite (IDimSocketNotify * notify, void * data, size_t bytes);

//===========================================================================
void DimSocketNewBuffer (DimSocketBuffer * out) {
    // all buffers full? create a new one
    if (s_numFull == s_rbuffers.size())
        CreateEmptyBuffer();
    // use the last partial or, if there aren't any, the first empty
    auto rbuf = s_numPartial
        ? s_rbuffers[s_numFull + s_numPartial - 1]
        : s_rbuffers[s_numFull];

    Buffer * buf = rbuf.base + rbuf.firstFree;
    if (buf) {
        rbuf.firstFree = buf->nextPos;
    } else {
        assert(rbuf.size < rbuf.reserved);
        buf = rbuf.base + rbuf.size;
        rbuf.size += 1;
    }
    buf->ownerPos = int(buf - rbuf.base);
    rbuf.used += 1;

    // set pointer to just passed the header
    out->data = buf + 1;
    out->bytes = rbuf.bufferSize - sizeof(*buf);

    // if the registered buffer is full move it to the back of the list
    if (rbuf.used == rbuf.reserved) {
        if (s_numPartial > 1)
            swap(rbuf, s_rbuffers[s_numFull]);
        s_numFull += 1;
        s_numPartial -= 1;
    } else if (rbuf.used == 1) {
        // no longer empty: it's in the right place, just update the count
        s_numPartial += 1;
    }
}

//===========================================================================
void DimSocketDeleteBuffer (void * ptr) {
    // get the header 
    Buffer * buf = (Buffer *) ptr - 1;
    assert((size_t) buf->ownerPos < s_rbuffers.size());
    int rbufPos = buf->ownerPos;
    auto rbuf = s_rbuffers[rbufPos];
    // buffer must be aligned within the owning registered buffer
    assert(buf >= rbuf.base && buf < rbuf.base + rbuf.size);
    assert(((char *) buf - (char *) rbuf.base) % rbuf.bufferSize == 0);

    buf->nextPos = rbuf.firstFree;
    rbuf.firstFree = int(buf - rbuf.base);
    rbuf.used -= 1;

    if (rbuf.used == rbuf.reserved - 1) {
        // no longer full: move to partial list
        if (rbufPos != s_numFull - 1)
            swap(rbuf, s_rbuffers[s_numFull - 1]);
        s_numFull -= 1;
        s_numPartial += 1;
    } else if (!rbuf.used) {
        // newly empty: move to empty list
        rbuf.lastUsed = DimClock::now();
        int pos = s_numFull + s_numPartial - 1;
        if (rbufPos != pos)
            swap(rbuf, s_rbuffers[pos]);
        s_numPartial -= 1;

        // over half the rbufs are empty? destroy one
        if (s_rbuffers.size() > 2 * (s_numFull + s_numPartial))
            DestroyEmptyBuffer();
    }
}
