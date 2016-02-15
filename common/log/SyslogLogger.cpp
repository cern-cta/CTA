/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/exception/Exception.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/Utils.hpp"

#include <errno.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::log::SyslogLogger::SyslogLogger(const std::string &socketName,
  const std::string &programName, const int logMask):
  Logger(programName),
  m_socketName(socketName),
  m_logMask(logMask),
  m_maxMsgLen(determineMaxMsgLen()),
  m_logFile(-1),
  m_priorityToText(generatePriorityToTextMap()),
  m_configTextToPriority(generateConfigTextToPriorityMap()) {
  struct stat fileStatus;
  bzero(&fileStatus, sizeof(fileStatus));
  if(stat(socketName.c_str(), &fileStatus)) {
    const std::string errMsg = cta::utils::errnoToString(errno);
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate syslog logger: Failed to stat"
      " socket \"" << socketName << "\"" ": " << errMsg;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// determineMaxMsgLen
//------------------------------------------------------------------------------
size_t cta::log::SyslogLogger::determineMaxMsgLen() {
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
  cta::log::SyslogLogger::generatePriorityToTextMap() {
  std::map<int, std::string> m;

  try {
    m[LOG_EMERG]   = "Emerg";
    m[LOG_ALERT]   = "Alert";
    m[LOG_CRIT]    = "Crit";
    m[LOG_ERR]     = "Error";
    m[LOG_WARNING] = "Warn";
    m[LOG_NOTICE]  = "Notice";
    m[LOG_INFO]    = "Info";
    m[LOG_DEBUG]   = "Debug";
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
  cta::log::SyslogLogger::generateConfigTextToPriorityMap() {
  std::map<std::string, int> m;

  try {
    m["LOG_EMERG"]   = LOG_EMERG;
    m["LOG_ALERT"]   = LOG_ALERT;
    m["LOG_CRIT"]    = LOG_CRIT;
    m["LOG_ERR"]     = LOG_ERR;
    m["LOG_WARNING"] = LOG_WARNING;
    m["LOG_NOTICE"]  = LOG_NOTICE;
    m["LOG_INFO"]    = LOG_INFO;
    m["LOG_DEBUG"]   = LOG_DEBUG;
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
// destructor
//------------------------------------------------------------------------------
cta::log::SyslogLogger::~SyslogLogger() {
  closeLog();
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::SyslogLogger::prepareForFork() {
  threading::MutexLocker lock(m_mutex);
  closeLog();
}

//------------------------------------------------------------------------------
// openLog
//------------------------------------------------------------------------------
void cta::log::SyslogLogger::openLog() {
  if(-1 != m_logFile) {
    return;
  }

  {
    struct sockaddr_un syslogAddr;
    syslogAddr.sun_family = AF_UNIX;
    strncpy(syslogAddr.sun_path, m_socketName.c_str(),
      sizeof(syslogAddr.sun_path));
    m_logFile = socket(AF_UNIX, SOCK_DGRAM, 0);
  }
  if(-1 == m_logFile) {
    return;
  }

  if(-1 == fcntl(m_logFile, F_SETFD, FD_CLOEXEC)) {
    close(m_logFile);
    m_logFile = -1;
    return;
  }

  {
    struct sockaddr_un syslogAddr;
    syslogAddr.sun_family = AF_UNIX;
    strncpy(syslogAddr.sun_path, m_socketName.c_str(),
      sizeof(syslogAddr.sun_path));
    if(-1 == connect(m_logFile, (struct sockaddr *)&syslogAddr,
      sizeof(syslogAddr))) {
      close(m_logFile);
      m_logFile = -1;
      return;
    }
  }

#ifdef __APPLE__
  {
    // MAC has has no MSG_NOSIGNAL
    // but >= 10.2 comes with SO_NOSIGPIPE
    int set = 1;
    if(0 != setsockopt(m_logFile, SOL_SOCKET, SO_NOSIGPIPE, &set,
      sizeof(int))) {
      close(m_logFile);
      m_logFile = -1;
      return;
    }
  }
#endif
}

//------------------------------------------------------------------------------
// closeLog
//------------------------------------------------------------------------------
void cta::log::SyslogLogger::closeLog() {
  if(-1 == m_logFile) {
    return;
  }
  close(m_logFile);
  m_logFile = -1;
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void cta::log::SyslogLogger::operator() (
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
void cta::log::SyslogLogger::writeLogMsg(
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

  // Terminate the string
  os << "\n";
}

//-----------------------------------------------------------------------------
// writeHeader
//-----------------------------------------------------------------------------
void cta::log::SyslogLogger::writeHeader(
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
std::string cta::log::SyslogLogger::cleanString(const std::string &s,
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

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void cta::log::SyslogLogger::reducedSyslog(std::string msg) {
  // Truncate the log message if it exceeds the permitted maximum
  if(msg.length() > m_maxMsgLen) {
    msg.resize(m_maxMsgLen);
    msg[msg.length() - 1] = '\n';
  }

  int send_flags = MSG_NOSIGNAL;

  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Try to connect if not already connected
  openLog();

  // If connected
  if(-1 != m_logFile) {
    // If sending the log message fails then try to reopen the syslog
    // connection and try again
    if(0 > send(m_logFile, msg.c_str(), msg.length(), send_flags)) {
      closeLog();
      openLog();
      if (-1 != m_logFile) {
        // If the second attempt to send the log message fails then give up and
        // attempt re-open next time
        if(0 > send(m_logFile, msg.c_str(), msg.length(), send_flags)) {
          closeLog();
        }
      }
    }
  }
}
