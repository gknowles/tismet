// httpmsg.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


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

struct HttpMsg::HdrValue {
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
*   HttpMsg::Hdr
*
***/


/****************************************************************************
*
*   HttpMsg::HdrIterator
*
***/


/****************************************************************************
*
*   HttpMsg::HdrRange
*
***/


/****************************************************************************
*
*   HttpMsg
*
***/

//===========================================================================
void HttpMsg::addHeader (HttpHdr id, const char value[]) {
}

//===========================================================================
void HttpMsg::addHeader (const char name[], const char value[]) {
}

//===========================================================================
CharBuf * HttpMsg::body () {
    return &m_data;
}

//===========================================================================
const CharBuf * HttpMsg::body () const {
    return &m_data;
}

//===========================================================================
ITempHeap & HttpMsg::heap () {
    return m_heap;
}


/****************************************************************************
*
*   HttpRequest
*
***/

bool HttpRequest::checkPseudoHeaders () const {
    const int must = kFlagHasMethod | kFlagHasScheme | kFlagHasPath;
    const int mustNot = kFlagHasStatus;
    return (m_flags & must) == must && (~m_flags & mustNot);
}

} // namespace
