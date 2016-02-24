// str.h - dim core
#ifndef DIM_STR_INCLUDED
#define DIM_STR_INCLUDED

#include "dim/config.h"

namespace Dim {

size_t strHash (const char src[]);

// calculates hash up to trailing null or maxlen, whichever comes first
size_t strHash (const char src[], size_t maxlen);

} // namespace

#endif
