// tsdata.h - tismet
#pragma once

#include "dim/handle.h"


/****************************************************************************
*
*   Declarations
*
***/

struct TsdFileHandle : Dim::HandleBase {};

TsdFileHandle tsdOpen(const std::string & name);

void tsdClose(TsdFileHandle file);
