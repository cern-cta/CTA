/******************************************************************************
 *                      castor/log/SyslogLogger.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/log/SyslogLogger.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/getconfent.h"

#include <errno.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::SyslogLogger::SyslogLogger(
  const std::string &programName)
  throw(castor::exception::Exception, castor::exception::InvalidArgument):
  Logger(programName),
  m_maxMsgLen(determineMaxMsgLen()),
  m_logFile(-1),
  m_priorityToText(generatePriorityToTextMap()),
  m_configTextToPriority(generateConfigTextToPriorityMap()) {
  initMutex();
}

//------------------------------------------------------------------------------
// determineMaxMsgLen
//------------------------------------------------------------------------------
size_t castor::log::SyslogLogger::determineMaxMsgLen() const throw() {
  const char *p = NULL;
  size_t msgSize = 0;

  if((p = getconfent("LOG", "MaxMessageSize", 0)) != NULL) {
    msgSize = atoi(p);
  } else {
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
  }

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
  castor::log::SyslogLogger::generatePriorityToTextMap() const {
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
    castor::exception::Exception ex;
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
  castor::log::SyslogLogger::generateConfigTextToPriorityMap() const {
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
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to generate configuration text to priority mapping: " <<
      se.what();
    throw ex;
  }

  return m;
}

//------------------------------------------------------------------------------
// initMutex
//------------------------------------------------------------------------------
void castor::log::SyslogLogger::initMutex() {
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if(0 != rc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to initialize mutex attribute for m_mutex: " <<
      sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
  if(0 != rc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set mutex type of m_mutex: " <<
      sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutex_init(&m_mutex, NULL);
   if(0 != rc) {
     castor::exception::Exception ex;
     ex.getMessage() << "Failed to initialize m_mutex: " << sstrerror(rc);
     throw ex;
   }
  rc = pthread_mutexattr_destroy(&attr);
  if(0 != rc) {
    pthread_mutex_destroy(&m_mutex);
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to destroy mutex attribute of m_mutex: " <<
      sstrerror(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::log::SyslogLogger::~SyslogLogger() throw() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void castor::log::SyslogLogger::prepareForFork() {
  // Enter critical section
  {
    const int mutex_lock_rc = pthread_mutex_lock(&m_mutex);
    if(0 != mutex_lock_rc) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to lock mutex of logger's critcial section: "
        << sstrerror(mutex_lock_rc);
      throw ex;
    }
  }

  closeLog();

  // Leave critical section.
  {
    const int mutex_unlock_rc = pthread_mutex_unlock(&m_mutex);
    if(0 != mutex_unlock_rc) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to unlock mutex of logger's critcial section: "
        << sstrerror(mutex_unlock_rc);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// openLog
//------------------------------------------------------------------------------
void castor::log::SyslogLogger::openLog() throw() {
  if(-1 != m_logFile) {
    return;
  }

  {
    struct sockaddr_un syslogAddr;
    syslogAddr.sun_family = AF_UNIX;
    strncpy(syslogAddr.sun_path, _PATH_LOG, sizeof(syslogAddr.sun_path));
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
    strncpy(syslogAddr.sun_path, _PATH_LOG, sizeof(syslogAddr.sun_path));
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
void castor::log::SyslogLogger::closeLog() throw() {
  if(-1 == m_logFile) {
    return;
  }
  close(m_logFile);
  m_logFile = -1;
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::vector<Param> &params,
  const struct timeval &timeStamp) throw() {
  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params,
  const struct timeval &timeStamp) throw() {
  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[],
  const struct timeval &timeStamp) throw() {
  operator() (priority, msg, params, params+numParams, timeStamp);
}

//-----------------------------------------------------------------------------
// buildSyslogHeader
//-----------------------------------------------------------------------------
std::string castor::log::SyslogLogger::buildSyslogHeader(
  const int priority,
  const struct timeval &timeStamp,
  const int pid) const throw() {
  char buf[80];
  int bufLen = sizeof(buf);
  int len = 0;
  std::ostringstream oss;

  oss << "<" << priority << ">";

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
  oss << buf << m_programName.c_str() << "[" << pid << "]: ";
  return oss.str();
}

//-----------------------------------------------------------------------------
// cleanString
//-----------------------------------------------------------------------------
std::string castor::log::SyslogLogger::cleanString(const std::string &s,
  const bool replaceUnderscores) const throw() {
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
void castor::log::SyslogLogger::reducedSyslog(std::string msg) throw() {
  // Truncate the log message if it exceeds the permitted maximum
  if(msg.length() > m_maxMsgLen) {
    msg.resize(m_maxMsgLen);
    msg[msg.length() - 1] = '\n';
  }

  int send_flags = 0;
#ifndef __APPLE__
  // MAC has has no MSG_NOSIGNAL
  // but >= 10.2 comes with SO_NOSIGPIPE
  send_flags = MSG_NOSIGNAL;
#endif
  // enter critical section
  const int mutex_lock_rc = pthread_mutex_lock(&m_mutex);
  // Do nothing if we failed to enter the critical section
  if(0 != mutex_lock_rc) {
    return;
  }

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

  // Leave critical section.
  pthread_mutex_unlock(&m_mutex);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::vector<Param> &params) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[]) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  operator() (priority, msg, numParams, params, timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::SyslogLogger::operator() (
  const int priority,
  const std::string &msg) throw() {

  Param *emptyParams = NULL;
  operator() (priority, msg, 0, emptyParams);
}

//------------------------------------------------------------------------------
// logMask
//------------------------------------------------------------------------------
int castor::log::SyslogLogger::logMask() const throw() {
  const char *const p = getconfent("LogMask", m_programName.c_str(), 0);

  // If the configuration file defines the log mask to use
  if (p != NULL) {
    // Try to find the corresponding integer priority value
    std::map<std::string, int>::const_iterator itor =
      m_configTextToPriority.find(p);

    // Return the priority if it was found else return the default INFO level
    if(m_configTextToPriority.end() != itor) {
      return itor->second;
    } else {
      return LOG_INFO;
    }
  }

  // If the priority wasn't found, default is INFO level
  return LOG_INFO;
}
