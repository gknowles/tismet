// tempheap.h - dim services
#pragma once

#include "dim/config.h"

#include <cstring>

namespace Dim {

/****************************************************************************
*
*   Temp heap interface
*
***/

class ITempHeap {
public:
    virtual ~ITempHeap () {}

    template <typename T, typename... Args>
    T * emplace (Args&&... args);
    template <typename T>
    T * alloc (size_t num);

    char * strDup (const char src[]);
    char * strDup (
        const char src[], 
        size_t len          // does not include null terminator
    );

    char * alloc (size_t bytes);
    virtual char * alloc (size_t bytes, size_t align) = 0;
};

//===========================================================================
template <typename T, typename... Args>
inline T * ITempHeap::emplace (Args&&... args) {
    char * tmp = alloc(sizeof(T), alignof(T));
    return new(tmp) T(args...);
}

//===========================================================================
template <typename T>
inline T * ITempHeap::alloc (size_t num) {
    char * tmp = alloc(num * sizeof(T), alignof(T));
    return new(tmp) T[num];
}

//===========================================================================
inline char * ITempHeap::strDup (const char src[]) {
    size_t len = std::strlen(src);
    return strDup(src, len);
}

//===========================================================================
inline char * ITempHeap::strDup (const char src[], size_t len) {
    char * out = alloc(sizeof(*src) * (len + 1), alignof(char));
    std::memcpy(out, src, len);
    out[len] = 0;
    return out;
}

//===========================================================================
inline char * ITempHeap::alloc (size_t bytes) {
    return alloc(bytes, alignof(char));
}


/****************************************************************************
*
*   TempHeap
*
***/

class TempHeap : public ITempHeap {
public:
    ~TempHeap ();
    void clear ();

    // ITempHeap
    char * alloc (size_t bytes, size_t align) override;

private:
    void * m_buffer{nullptr};
};

} // namespace
