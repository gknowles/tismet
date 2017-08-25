// queryparseint.h
// Generated by pargen 2.1.2 at 2017-08-24T15:50:26-0700
// clang-format off
#pragma once

#include "queryparsebaseint.h"


/****************************************************************************
*
*   QueryParser
*
***/

class QueryParser : public QueryParserBase {
public:
    using QueryParserBase::QueryParserBase;

    bool parse (const char src[]);
    size_t errpos () const { return m_errpos; }

private:
    bool stateNumber (const char *& src);
    bool stateQuery (const char *& src);

    // Events
    bool onArgNumEnd ();
    bool onArgQueryStart ();
    bool onExpMinusEnd ();
    bool onExpNumChar (char ch);
    bool onFnMaximumAboveStart ();
    bool onFnSumStart ();
    bool onFracNumChar (char ch);
    bool onFuncEnd ();
    bool onIntChar (char ch);
    bool onMinusEnd ();
    bool onPathStart (const char * ptr);
    bool onPathEnd (const char * eptr);
    bool onPathSegStart (const char * ptr);
    bool onPathSegEnd (const char * eptr);
    bool onSclRangeEndChar (char ch);
    bool onSclSingleChar (char ch);
    bool onSegBlotEnd ();
    bool onSegCharListEnd ();
    bool onSegLiteralStart (const char * ptr);
    bool onSegLiteralEnd (const char * eptr);
    bool onSegStrListStart ();
    bool onSegStrListEnd ();
    bool onSegStrValStart (const char * ptr);
    bool onSegStrValEnd (const char * eptr);

    // Data members
    size_t m_errpos{0};
};
