// log.h - dim core
#ifndef DIM_LOG_INCLUDED
#define DIM_LOG_INCLUDED

#include "dim/config.h"

#include <sstream>
#include <string>

namespace Dim {

enum LogType {
    kLogDebug,
    kLogInfo,
    kLogError,
    kLogCrash,
};

namespace Detail {

class LogInternal;

class Log : public std::ostringstream {
public:
    Log (const LogInternal & from);
    ~Log ();

protected:
    LogType m_type;
};

class LogCrash : public Log {
public:
    using Log::Log;
    /* [[noreturn]] */ ~LogCrash ();
};

} // namespace

Detail::Log logMsgDebug ();
Detail::Log logMsgInfo ();
Detail::Log logMsgError ();
Detail::LogCrash logMsgCrash ();


class ILogNotify {
public:
    virtual void onLog (LogType type, const std::string & msg) = 0;
};

void logAddNotify (ILogNotify * notify);

} // namespace

#endif
