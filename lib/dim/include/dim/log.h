// log.h - dim core
#pragma once

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

class ILogNotify {
public:
    virtual void onLog (LogType type, const std::string & msg) = 0;
};

void logAddNotify (ILogNotify * notify);


namespace Detail {

class Log : public std::ostringstream {
public:
    Log (LogType type);
    ~Log ();

private:
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


} // namespace
