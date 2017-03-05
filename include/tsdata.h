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

bool tsdFindMetric(uint32_t & out, TsdFileHandle h, const std::string & name);
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, const std::string & name);
