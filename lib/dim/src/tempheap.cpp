// tempheap.cpp - dim core
#include "pch.h"
#pragma hdrstop


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kBufferSize = 4096;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

struct Buffer {
    Buffer * m_next;
    size_t m_avail;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/



/****************************************************************************
*
*   DimTempHeap
*
***/

//===========================================================================
DimTempHeap::~DimTempHeap () {
    Buffer * ptr = (Buffer *) m_buffer;
    while (ptr) {
        Buffer * next = ptr->m_next;
        free(ptr);
        ptr = next;
    }
}

//===========================================================================
char * DimTempHeap::Alloc (size_t bytes, size_t align) {
    for (;;) {
        Buffer * buf = (Buffer *) m_buffer;
        if (buf) {
            void * ptr = (char *) buf + kBufferSize - buf->m_avail;
            if (std::align(align, bytes, ptr, buf->m_avail)) {
                buf->m_avail -= bytes;
                return (char *) ptr;
            }
        }
        if (bytes > kBufferSize / 3) {
            Buffer * tmp = (Buffer *) malloc(sizeof(Buffer) + bytes + align);
            tmp->m_next = buf;
            tmp->m_avail = bytes + align;
            m_buffer = tmp;
        } else {
            Buffer * tmp = (Buffer *) malloc(kBufferSize);
            tmp->m_next = buf;
            tmp->m_avail = kBufferSize - sizeof(Buffer);
            m_buffer = tmp;
        }
    }
}