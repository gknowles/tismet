// log.h - dim core
#ifndef DIM_LOG_INCLUDED
#define DIM_LOG_INCLUDED

#include "dim/config.h"

namespace Dim {

enum LogSeverity {
    kDebug,
    kInfo,
    kError,
    kCrash,
};

class Log : public std::ostringstream {
    LogSeverity m_severity;
public:
    Log (LogSeverity severity) : m_severity(severity) {}
    ~Log ();
};


class ILogNotify {
public:
    virtual void onLog (
        LogSeverity severity,
        const std::string & msg
    ) = 0;
};

void logAddNotify (ILogNotify * notify);

} // namespace

#endif
