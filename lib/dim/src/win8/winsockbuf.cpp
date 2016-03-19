// winsockbuf.cpp - dim core - windows platform
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/

// includes space for BufferSlice header
const unsigned kDefaultBufferSliceSize = 4096;


/****************************************************************************
*
*   Private declarations
*
***/

static void destroyBufferSlice (void * ptr);

//===========================================================================
namespace {

struct BufferSlice {
    union {
        int ownerPos;
        int nextPos;
    };
};
struct Buffer {
    RIO_BUFFERID id;
    char * base;
    TimePoint lastUsed;
    int sliceSize;
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

static int s_sliceSize{kDefaultBufferSliceSize};
static size_t s_bufferSize{256 * kDefaultBufferSliceSize};
static size_t s_minLargeAlloc;
static size_t s_minAlloc;
static size_t s_pageSize;

// buffers are kept sorted by state (full, partial, empty)
static vector<Buffer> s_buffers;
static int s_numPartial;
static int s_numFull;
static mutex s_mut;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static BufferSlice * getSlice (const Buffer & buf, int pos) {
    char * ptr = (char *) buf.base + pos * buf.sliceSize;
    return (BufferSlice *) ptr;
}

//===========================================================================
static void findBufferSlice (
    BufferSlice ** sliceOut, 
    Buffer ** bufferOut, 
    void * ptr
) {
    auto * slice = (BufferSlice *) ptr - 1;
    assert((size_t) slice->ownerPos < s_buffers.size());
    int rbufPos = slice->ownerPos;
    auto * buf = &s_buffers[rbufPos];
    // BufferSlice must be aligned within the owning registered buffer
    assert((char *) slice >= buf->base && slice < getSlice(*buf, buf->size));
    assert(((char *) slice - buf->base) % buf->sliceSize == 0);

    *sliceOut = slice;
    *bufferOut = buf;
}

//===========================================================================
static void createEmptyBuffer () {
    size_t bytes = s_bufferSize;
    size_t granularity = s_minAlloc;
    if (s_minLargeAlloc && s_bufferSize >= s_minLargeAlloc) {
        granularity = s_minLargeAlloc;
    }
    // round up, but not to exceed DWORD
    bytes += granularity - 1;
    bytes = bytes - bytes % granularity;
    if (bytes > numeric_limits<DWORD>::max()) {
        bytes = numeric_limits<DWORD>::max() / granularity * granularity;
    }

    s_buffers.emplace_back();
    Buffer & buf = s_buffers.back();
    buf.sliceSize = s_sliceSize;
    buf.reserved = int(bytes / s_sliceSize);
    buf.lastUsed = TimePoint::min();
    buf.size = 0;
    buf.used = 0;
    buf.firstFree = 0;
    buf.base = (char *) VirtualAlloc(
        nullptr,
        bytes,
        MEM_COMMIT 
            | MEM_RESERVE 
            | (bytes > s_minLargeAlloc ? MEM_LARGE_PAGES : 0),
        PAGE_READWRITE
    );
    assert(buf.base);

    buf.id = s_rio.RIORegisterBuffer(buf.base, (DWORD) bytes);
    if (buf.id == RIO_INVALID_BUFFERID) {
        logMsgCrash() << "RIORegisterBuffer failed, " 
            << WinError();
    }
}

//===========================================================================
static void destroyEmptyBuffer () {
    assert(!s_buffers.empty());
    Buffer & buf = s_buffers.back();
    assert(!buf.used);
    s_rio.RIODeregisterBuffer(buf.id);
    VirtualFree(buf.base, 0, MEM_RELEASE);
    s_buffers.pop_back();
}

//===========================================================================
static void destroyBufferSlice (void * ptr) {
    // get the header 
    BufferSlice * slice;
    Buffer * pbuf;
    findBufferSlice(&slice, &pbuf, ptr);
    auto & buf = *pbuf;
    int rbufPos = slice->ownerPos;

    slice->nextPos = buf.firstFree;
    buf.firstFree = int((char *) slice - buf.base) / buf.sliceSize;
    buf.used -= 1;

    if (buf.used == buf.reserved - 1) {
        // no longer full: move to partial list
        if (rbufPos != s_numFull - 1)
            swap(buf, s_buffers[s_numFull - 1]);
        s_numFull -= 1;
        s_numPartial += 1;
    } else if (!buf.used) {
        // newly empty: move to empty list
        buf.lastUsed = Clock::now();
        int pos = s_numFull + s_numPartial - 1;
        if (rbufPos != pos)
            swap(buf, s_buffers[pos]);
        s_numPartial -= 1;

        // over half the rbufs are empty? destroy one
        if (s_buffers.size() > 2 * (s_numFull + s_numPartial))
            destroyEmptyBuffer();
    }
}


/****************************************************************************
*
*   SocketBuffer
*
***/

//===========================================================================
SocketBuffer::~SocketBuffer () {
    destroyBufferSlice(data);
}


/****************************************************************************
*
*   Shutdown
*
***/

namespace {
    class Shutdown : public IAppShutdownNotify {
        bool onAppQueryConsoleDestroy () override;
    };
} // namespace
static Shutdown s_cleanup;

//===========================================================================
bool Shutdown::onAppQueryConsoleDestroy () {
    lock_guard<mutex> lk{s_mut};

    while (!s_buffers.empty())
        destroyEmptyBuffer();

    return true;
}


/****************************************************************************
*
*   Internal API
*
***/

//===========================================================================
void iSocketBufferInitialize (RIO_EXTENSION_FUNCTION_TABLE & rio) {
    appMonitorShutdown(&s_cleanup);

    s_rio = rio;

    s_minLargeAlloc = GetLargePageMinimum();
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    s_pageSize = info.dwPageSize;
    s_minAlloc = info.dwAllocationGranularity;

    // if large pages are available make sure the buffers are at least 
    // that big
    s_bufferSize = max(s_minLargeAlloc, s_bufferSize);
}

//===========================================================================
void iSocketGetRioBuffer (
    RIO_BUF * out, 
    SocketBuffer * sbuf,
    size_t bytes
) {
    lock_guard<mutex> lk{s_mut};

    assert(bytes <= sbuf->len);
    BufferSlice * slice;
    Buffer * pbuf;
    findBufferSlice(&slice, &pbuf, sbuf->data);
    out->BufferId = pbuf->id;
    out->Offset = ULONG((char *) sbuf->data - (char *) pbuf->base);
    out->Length = (int) bytes;
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
unique_ptr<SocketBuffer> socketGetBuffer () {
    lock_guard<mutex> lk{s_mut};

    // all buffers full? create a new one
    if (s_numFull == s_buffers.size())
        createEmptyBuffer();
    // use the last partial or, if there aren't any, the first empty
    auto & buf = s_numPartial
        ? s_buffers[s_numFull + s_numPartial - 1]
        : s_buffers[s_numFull];

    BufferSlice * slice;
    if (buf.used < buf.size) {
        slice = getSlice(buf, buf.firstFree);
        buf.firstFree = slice->nextPos;
    } else {
        assert(buf.size < buf.reserved);
        slice = getSlice(buf, buf.size);
        buf.size += 1;
    }
    slice->ownerPos = int(&buf - s_buffers.data());
    buf.used += 1;

    // set pointer to just passed the header
    auto out = make_unique<SocketBuffer>();
    out->data = (char *) (slice + 1);
    out->len = buf.sliceSize - sizeof(*slice);

    // if the registered buffer is full move it to the back of the list
    if (buf.used == buf.reserved) {
        if (s_numPartial > 1)
            swap(buf, s_buffers[s_numFull]);
        s_numFull += 1;
        s_numPartial -= 1;
    } else if (buf.used == 1) {
        // no longer empty: it's in the right place, just update the count
        s_numPartial += 1;
    }

    return out;
}

} // namespace
