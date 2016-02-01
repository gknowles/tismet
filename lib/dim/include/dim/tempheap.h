// tempheap.h - dim services
#ifndef DIM_TEMPHEAP_INCLUDED
#define DIM_TEMPHEAP_INCLUDED

#include "dim/config.h"

#include <cstring>


/****************************************************************************
*
*   Temp heap interface
*
***/

class IDimTempHeap {
public:
    virtual ~IDimTempHeap () {}

    template <typename T, typename... Args>
    T * New (Args&&... args);
    template <typename T>
    T * Alloc (size_t num);

    char * StrDup (const char src[]);
    char * StrDup (
        const char src[], 
        size_t len          // does not include null terminator
    );

    char * Alloc (size_t bytes);
    virtual char * Alloc (size_t bytes, size_t align) = 0;
};

//===========================================================================
template <typename T, typename... Args>
inline T * IDimTempHeap::New (Args&&... args) {
    char * tmp = Alloc(sizeof(T), alignof(T));
    return new(tmp) T(args);
}

//===========================================================================
template <typename T>
inline T * IDimTempHeap::Alloc (size_t num) {
    char * tmp = Alloc(num * sizeof(T), alignof(T));
    return new(tmp) T[num];
}

//===========================================================================
inline char * IDimTempHeap::StrDup (const char src[]) {
    size_t len = std::strlen(src);
    return StrDup(src, len);
}

//===========================================================================
inline char * IDimTempHeap::StrDup (const char src[], size_t len) {
    char * out = Alloc(sizeof(*src) * (len + 1), alignof(char));
    std::memcpy(out, src, len);
    out[len] = 0;
    return out;
}

//===========================================================================
inline char * IDimTempHeap::Alloc (size_t bytes) {
    return Alloc(bytes, alignof(char));
}


/****************************************************************************
*
*   DimTempHeap
*
***/

class DimTempHeap : public IDimTempHeap {
public:
    ~DimTempHeap ();

    // IDimTempHeap
    char * Alloc (size_t bytes, size_t align) override;

private:
    void * m_buffer{nullptr};
};

#endif
