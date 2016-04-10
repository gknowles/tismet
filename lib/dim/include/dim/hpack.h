// hpack.h - dim services
//
// implements hpack, as defined by:
//  rfc7541 - HPACK: Header Compression for HTTP/2
#pragma once

#include "dim/config.h"

#include <deque>
#include <string>

namespace Dim {

/****************************************************************************
*     
*   Common
*     
***/  

struct HpackFieldView;
enum HpackFlags {
    kNeverIndexed = 1,
};

struct HpackDynField {
    std::string name;
    std::string value;
};


/****************************************************************************
*     
*   HpackEncode
*     
***/  

class HpackEncode {
public:
    HpackEncode (size_t tableSize);
    void setTableSize (size_t tableSize);
    
    void startBlock (CharBuf * out);
    void endBlock ();

    void header (
        const char name[], 
        const char value[], 
        int flags = 0 // DimHpack::*
    );
    void header (
        HttpHdr name, 
        const char value[], 
        int flags = 0 // DimHpack::*
    );

private:
    void write (const char str[]);
    void write (const char str[], size_t len);
    void write (size_t val, char prefix, int prefixBits);

    size_t m_dynSize{0};
    CharBuf * m_out{nullptr};
};


/****************************************************************************
*     
*   HpackDecode
*     
***/  

class IHpackDecodeNotify {
public:
    virtual ~IHpackDecodeNotify () {}

    virtual void onHpackHeader (
        HttpHdr id,
        const char name[],
        const char value[],
        int flags // Flags::*
    ) = 0;
};

class HpackDecode {
public:
    HpackDecode (size_t tableSize);
    void reset ();
    void setTableSize (size_t tableSize);

    bool parse (
        IHpackDecodeNotify * notify,
        ITempHeap * heap,
        const char src[],
        size_t srcLen
    );

private:
    void pruneDynTable();

    bool readInstruction (
        IHpackDecodeNotify * notify, 
        ITempHeap * heap, 
        const char *& src, 
        size_t & srcLen
    );
    bool readIndexedField (
        HpackFieldView * out, 
        ITempHeap * heap, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool readIndexedName (
        HpackFieldView * out, 
        ITempHeap * heap, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool read (
        size_t * out, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool read (
        const char ** out,
        ITempHeap * heap, 
        const char *& src, 
        size_t & srcLen
    );

    size_t m_dynSize{0};
    std::deque<HpackDynField> m_dynTable;
    size_t m_dynUsed{0};
};

} // namespace
