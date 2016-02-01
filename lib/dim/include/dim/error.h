// error.h - dim core
#ifndef DIM_ERROR_INCLUDED
#define DIM_ERROR_INCLUDED

#include "dim/config.h"

enum DimLogSeverity {
    kDebug,
    kInfo,
    kError,
    kCrash,
};

class DimLog : public std::ostringstream {
    DimLogSeverity m_severity;
public:
    DimLog (DimLogSeverity severity) : m_severity(severity) {}
    ~DimLog ();
};


class IDimLogNotify {
public:
    virtual void OnLog (
        DimLogSeverity severity,
        const std::string & msg
    ) = 0;
};

void DimLogRegisterHandler (IDimLogNotify * notify);

#endif
