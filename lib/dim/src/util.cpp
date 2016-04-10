// util.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Variables
*
***/

// randomly generated key
static uint8_t s_key[] = {
    0xed, 0x2e, 0x24, 0x9b, 0x49, 0x5f, 0x10, 0xf8,
    0xb8, 0xca, 0x13, 0x3e, 0xb4, 0x99, 0x56, 0x5d,
};
static_assert(size(s_key) == crypto_shorthash_KEYBYTES, "");


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
size_t strHash (const char src[]) {
    int64_t hash;
    crypto_shorthash(
        (uint8_t *) &hash, 
        (uint8_t *) src, 
        strlen(src), 
        s_key
    );
    return hash;
}

//===========================================================================
size_t strHash (const char src[], size_t maxlen) {
    size_t len = min(strlen(src), maxlen);
    int64_t hash;
    crypto_shorthash(
        (uint8_t *) &hash, 
        (uint8_t *) src, 
        len, 
        s_key
    );
    return hash;
}

} // namespace
