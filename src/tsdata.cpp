// tsdata.cpp - tismet tsdata
#include "pch.h"
#pragma hdrstop

using namespace std;
using namespace Dim;


/****************************************************************************
*
*   Private
*
***/

struct TsdFile {
};


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<TsdFileHandle, TsdFile> s_files;



/****************************************************************************
*
*   External
*
***/

//===========================================================================
TsdFileHandle tsdOpen(const std::string & name) {
    auto * file = new TsdFile;
    auto h = s_files.insert(file);
    return h;
}

//===========================================================================
void tsdClose(TsdFileHandle h) {
    s_files.erase(h);
}
