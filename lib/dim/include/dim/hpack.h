// hpack.h - dim services
//
// implements hpack, as defined by:
//  rfc7541 - HPACK: Header Compression for HTTP/2

#ifndef DIM_HPACK_INCLUDED
#define DIM_HPACK_INCLUDED

#include "dim/config.h"

#include <cassert>
#include <deque>

//===========================================================================
namespace DimHpack {
//===========================================================================

/****************************************************************************
*     
*   Common
*     
***/  

struct FieldView;
enum Flags {
    kNeverIndexed = 1,
};

struct DynField {
    std::string name;
    std::string value;
};


/****************************************************************************
*     
*   Encode
*     
***/  

class Encode {
public:
    Encode (size_t tableSize);
    void SetTableSize (size_t tableSize);
    
    void StartBlock (CharBuf * out);
    void EndBlock ();

    void Header (
        const char name[], 
        const char value[], 
        int flags = 0 // DimHpack::*
    );
    void Header (
        HttpHdr name, 
        const char value[], 
        int flags = 0 // DimHpack::*
    );

private:
    void Write (const char str[]);
    void Write (const char str[], size_t len);
    void Write (size_t val, char prefix, int prefixBits);

    size_t m_dynSize{0};
    CharBuf * m_out{nullptr};
};


/****************************************************************************
*     
*   Decode
*     
***/  

class IDecodeNotify {
public:
    virtual ~IDecodeNotify () {}

    virtual void OnHpackHeader (
        HttpHdr id,
        const char name[],
        const char value[],
        int flags // Flags::*
    ) = 0;
};

class Decode {
public:
    Decode (size_t tableSize);
    void Reset ();
    void SetTableSize (size_t tableSize);

    bool Parse (
        IDecodeNotify * notify,
        IDimTempHeap * heap,
        const char src[],
        size_t srcLen
    );

private:
    void PruneDynTable();

    bool ReadInstruction (
        IDecodeNotify * notify, 
        IDimTempHeap * heap, 
        const char *& src, 
        size_t & srcLen
    );
    bool ReadIndexedField (
        FieldView * out, 
        IDimTempHeap * heap, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool ReadIndexedName (
        FieldView * out, 
        IDimTempHeap * heap, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool Read (
        size_t * out, 
        size_t prefixBits, 
        const char *& src, 
        size_t & srcLen
    );
    bool Read (
        const char ** out,
        IDimTempHeap * heap, 
        const char *& src, 
        size_t & srcLen
    );

    size_t m_dynSize{0};
    std::deque<DynField> m_dynTable;
    size_t m_dynUsed{0};
};

//===========================================================================
} // namespace

#endif
