// tls.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private
*
***/

namespace {

class TlsConn {
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TlsConnHandle, TlsConn> s_conns;


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
TlsConnHandle tlsCreate ();

//===========================================================================
void tlsAddCipherSuite (
    TlsConnHandle h,
    const TlsCipherSuite suites[], 
    size_t count
);

//===========================================================================
void tlsClose (TlsConnHandle h);

} // namespace
