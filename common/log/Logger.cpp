/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include <sys/time.h>
#include <sys/syslog.h>
#include <sys/syscall.h>

namespace cta {
namespace log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Logger::Logger(const std::string &programName, const int logMask):
  m_programName(programName), m_logMask(logMask),
  m_maxMsgLen(determineMaxMsgLen()),
  m_priorityToText(generatePriorityToTextMap()) {}

//------------------------------------------------------------------------------
// getProgramName
//------------------------------------------------------------------------------
const std::string &Logger::getProgramName() const {
  return m_programName;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Logger::~Logger() {
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void Logger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params) {

  const std::string rawParams;
  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);
  const int pid = getpid();

  //-------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by
  // building the message ourselves. We then only call a reduced version of
  // syslog (namely reducedSyslog). The reason behind it is to be able to set
  // the message timestamp ourselves, in case we log messages asynchronously,
  // as we do when retrieving logs from the DB
  //-------------------------------------------------------------------------

  // Ignore messages whose priority is not of interest
  if(priority > m_logMask) {
    return;
  }

  // Try to find the textual representation of the syslog priority
  std::map<int, std::string>::const_iterator priorityTextPair =
    m_priorityToText.find(priority);

  // Do nothing if the log priority is not valid
  if(m_priorityToText.end() == priorityTextPair) {
    return;
  }

  // Safe to get a reference to the textual representation of the priority
  const std::string &priorityText = priorityTextPair->second;

  std::ostringstream os;

  writeLogMsg(
    os,
    priority,
    priorityText,
    msg,
    params,
    rawParams,
    timeStamp,
    m_programName,
    pid);

  reducedSyslog(os.str());
}


//-----------------------------------------------------------------------------
// writeLogMsg
//-----------------------------------------------------------------------------
void Logger::writeLogMsg(
  std::ostringstream &os,
  const int priority,
  const std::string &priorityText,
  const std::string &msg,
  const std::list<Param> &params,
  const std::string &rawParams,
  const struct timeval &timeStamp,
  const std::string &programName,
  const int pid) {

  //-------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by
  // building the message ourselves. We then only call a reduced version of
  // syslog (namely reducedSyslog). The reason behind it is to be able to set
  // the message timestamp ourselves, in case we log messages asynchronously,
  // as we do when retrieving logs from the DB
  //-------------------------------------------------------------------------

  // Start message with priority, time, program and PID (syslog standard
  // format)
  writeHeader(os, priority | LOG_LOCAL3, timeStamp, programName, pid);

  const int tid = syscall(__NR_gettid);

  // Append the log level, the thread id and the message text
  os << "LVL=\"" << priorityText << "\" TID=\"" << tid << "\" MSG=\"" <<
    msg << "\" ";

  // Process parameters
  for(auto itor = params.cbegin(); itor != params.cend(); itor++) {
    const Param &param = *itor;

    // Check the parameter name, if it's an empty string set the value to
    // "Undefined".
    const std::string name = param.getName() == "" ? "Undefined" :
      cleanString(param.getName(), true);

    // Process the parameter value
    const std::string value = cleanString(param.getValue(), false);

    // Write the name and value to the buffer
    os << name << "=\"" << value << "\" ";
  }

  // Append raw parameters
  os << rawParams;
}

//-----------------------------------------------------------------------------
// writeHeader
//-----------------------------------------------------------------------------
void Logger::writeHeader(
  std::ostringstream &os,
  const int priority,
  const struct timeval &timeStamp,
  const std::string &programName,
  const int pid) {
  char buf[80];
  int bufLen = sizeof(buf);
  int len = 0;

  os << "<" << priority << ">";

  struct tm localTime;
  localtime_r(&(timeStamp.tv_sec), &localTime);
  len += strftime(buf, bufLen, "%Y-%m-%dT%T", &localTime);
  len += snprintf(buf + len, bufLen - len, ".%06ld",
    (unsigned long)timeStamp.tv_usec);
  len += strftime(buf + len, bufLen - len, "%z: ", &localTime);
  // dirty trick to have the proper timezone format (':' between hh and mm)
  buf[len-2] = buf[len-3];
  buf[len-3] = buf[len-4];
  buf[len-4] = ':';
  buf[sizeof(buf) - 1] = '\0';
  os << buf << programName << "[" << pid << "]: ";
}

//-----------------------------------------------------------------------------
// cleanString
//-----------------------------------------------------------------------------
std::string Logger::cleanString(const std::string &s,
  const bool replaceUnderscores) {
  // Trim both left and right white-space
  std::string result = utils::trimString(s);
  
  for (std::string::iterator it = result.begin(); it != result.end(); ++it) {

    // Replace double quote with single quote
    if ('"' == *it) {
      *it = '\'';
    }
    
    // Replace newline and tab with a space
    if ('\t' == *it || '\n' == *it) {
      *it = ' ';
    }

    // If requested, replace spaces with underscores
    if(replaceUnderscores && ' ' == *it) {
      *it = '_';
    }
  }

  return result;
}

//------------------------------------------------------------------------------
// determineMaxMsgLen
//------------------------------------------------------------------------------
size_t Logger::determineMaxMsgLen() {
  size_t msgSize = 0;

  // Determine the size automatically, this is not guaranteed to work!
  FILE *const fp = fopen("/etc/rsyslog.conf", "r");
  if(fp) {
    char buffer[1024];

    // The /etc/rsyslog.conf file exists so we assume the default message
    // size of 2K.
    msgSize = DEFAULT_RSYSLOG_MSGLEN;

    // In rsyslog versions >= 3.21.4, the maximum size of a message became
    // configurable through the $MaxMessageSize global config directive.
    // Here we attempt to find out if the user has increased the size!
    while(fgets(buffer, sizeof(buffer), fp) != NULL) {
      if(strncasecmp(buffer, "$MaxMessageSize", 15)) {
        continue; // Option not of interest
      }
      msgSize = atol(&buffer[15]);
    }
    fclose(fp);
  }

  // If the /etc/rsyslog.conf file is missing which implies that we are
  // running on a stock syslogd system, therefore the message size is
  // governed by the syslog RFC: http://www.faqs.org/rfcs/rfc3164.html

  // Check that the size of messages falls within acceptable limits
  if((msgSize >= DEFAULT_SYSLOG_MSGLEN) && (msgSize <= LOG_MAX_LINELEN)) {
    return msgSize;
  } else {
    return DEFAULT_SYSLOG_MSGLEN;
  }
}

//------------------------------------------------------------------------------
// generatePriorityToTextMap
//------------------------------------------------------------------------------
std::map<int, std::string>
  Logger::generatePriorityToTextMap() {
  std::map<int, std::string> m;

  try {
    m[LOG_EMERG]   = "Emerg";
    m[ALERT]       = "Alert";
    m[LOG_CRIT]    = "Crit";
    m[ERR]         = "Error";
    m[WARNING]     = "Warn";
    m[LOG_NOTICE]  = "Notice";
    m[INFO]        = "Info";
    m[DEBUG]       = "Debug";
  } catch(std::exception &se) {
    exception::Exception ex;
    ex.getMessage() << "Failed to generate priority to text mapping: " <<
      se.what();
    throw ex;
  }

  return m;
}

//------------------------------------------------------------------------------
// generateConfigTextToPriorityMap
//------------------------------------------------------------------------------
std::map<std::string, int>
  Logger::generateConfigTextToPriorityMap() {
  std::map<std::string, int> m;

  try {
    m["LOG_EMERG"]   = LOG_EMERG;
    m["ALERT"]       = ALERT;
    m["LOG_CRIT"]    = LOG_CRIT;
    m["ERR"]         = ERR;
    m["WARNING"]     = WARNING;
    m["LOG_NOTICE"]  = LOG_NOTICE;
    m["INFO"]        = INFO;
    m["DEBUG"]       = DEBUG;
  } catch(std::exception &se) {
    exception::Exception ex;
    ex.getMessage() <<
      "Failed to generate configuration text to priority mapping: " <<
      se.what();
    throw ex;
  }

  return m;
}

//------------------------------------------------------------------------------
// setLogMask
//------------------------------------------------------------------------------
void Logger::setLogMask(const std::string logMask) {
  try {
    setLogMask(toLogLevel(logMask));
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string("Failed to set log mask: ") + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// setLogMask
//------------------------------------------------------------------------------
void Logger::setLogMask(const int logMask) {
  m_logMask = logMask;
}

} // namespace log
} // namespace cta
