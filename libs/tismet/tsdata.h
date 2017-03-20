// tsdata.h - tismet
#pragma once

#include "core/handle.h"


/****************************************************************************
*
*   Declarations
*
***/

struct TsdFileHandle : Dim::HandleBase {};

TsdFileHandle tsdOpen(std::string_view name);

void tsdClose(TsdFileHandle file);

bool tsdFindMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
bool tsdInsertMetric(uint32_t & out, TsdFileHandle h, std::string_view name);
