// error.h - dim core
#ifndef DIM_ERROR_INCLUDED
#define DIM_ERROR_INCLUDED

#include "dim/config.h"

enum DimErrorSeverity {
    kDebug,
    kInfo,
    kError,
    kFatal,
};

class DimErrorLog : public std::ostringstream {
    DimErrorSeverity m_severity;
public:
    DimErrorLog (DimErrorSeverity severity) : m_severity(severity) {}
    ~DimErrorLog ();
};

#endif
