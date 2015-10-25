// tempheap.h - dim services
#ifndef DIM_TEMPHEAP_INCLUDED
#define DIM_TEMPHEAP_INCLUDED

#include "dim/config.h"

#include <cstring>

class ITempHeap {
public:
    virtual ~ITempHeap () {}

    template <typename T, typename... Args>
    T * New (Args&&... args);
    template <typename T>
    T * Alloc (int num);

    char * StrDup (const char * src);

    char * Alloc (size_t bytes);
    virtual char * Alloc (size_t bytes, size_t align) = 0;
};

//===========================================================================
template <typename T, typename... Args>
inline T * ITempHeap::New (Args&&... args) {
    char * tmp = Alloc(sizeof(T), alignof(T));
    return new(tmp) T(args);
}

//===========================================================================
template <typename T>
inline T * ITempHeap::Alloc (int num) {
    char * tmp = Alloc(num * sizeof(T), alignof(T));
    return new(tmp) T[num];
}

//===========================================================================
inline char * ITempHeap::StrDup (const char * src) {
    size_t count = std::strlen(src);
    return Alloc(sizeof(*src) * count, alignof(char));
}

//===========================================================================
inline char * ITempHeap::Alloc (size_t bytes) {
    return Alloc(bytes, alignof(char));
}

#endif
