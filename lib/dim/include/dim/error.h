// error.h - dim core
#ifndef DIM_ERROR_INCLUDED
#define DIM_ERROR_INCLUDED

#include "dim/config.h"

enum DimErrorSeverity {
    kDebug,
    kInfo,
    kError,
    kCrash,
};

class DimLog : public std::ostringstream {
    DimErrorSeverity m_severity;
public:
    DimLog (DimErrorSeverity severity) : m_severity(severity) {}
    ~DimLog ();
};

#endif
