// httpmsg.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/


/****************************************************************************
*
*   Declarations
*
***/

struct DimHttpMsg::HdrValue {
    const char * value;
private:
    HdrValue * m_next{nullptr};
};


/****************************************************************************
*
*   Private
*
***/

namespace {


} // namespace


/****************************************************************************
*
*   DimHttpMsg::Hdr
*
***/


/****************************************************************************
*
*   DimHttpMsg::HdrIterator
*
***/


/****************************************************************************
*
*   DimHttpMsg::HdrRange
*
***/


/****************************************************************************
*
*   DimHttpMsg
*
***/

//===========================================================================
void DimHttpMsg::AddHeader (HttpHdr id, const char value[]) {
}

//===========================================================================
void DimHttpMsg::AddHeader (const char name[], const char value[]) {
}

//===========================================================================
CharBuf * DimHttpMsg::Body () {
    return &m_data;
}

//===========================================================================
const CharBuf * DimHttpMsg::Body () const {
    return &m_data;
}

//===========================================================================
IDimTempHeap & DimHttpMsg::Heap () {
    return m_heap;
}


/****************************************************************************
*
*   DimHttpRequestMsg
*
***/

bool DimHttpRequestMsg::CheckPseudoHeaders () const {
    const int must = kFlagHasMethod | kFlagHasScheme | kFlagHasPath;
    const int mustNot = kFlagHasStatus;
    return (m_flags & must) == must && (~m_flags & mustNot);
}
