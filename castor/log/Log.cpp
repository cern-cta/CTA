/******************************************************************************
 *                      Log.cpp
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

// Include Files
#include "castor/log/Constants.hpp"
#include "castor/log/Log.hpp"
#include "h/Castor_limits.h"
#include "h/getconfent.h"

#include <errno.h>
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
castor::log::Log::Log(const std::string &programName)
  throw(castor::exception::Internal, castor::exception::InvalidArgument):
  m_programName(programName),
  m_maxMsgLen(determineMaxMsgLen()),
  m_logFile(-1),
  m_connected(false),
  m_priorityToText(generatePriorityToTextMap()) {
  checkProgramNameLen(programName);
  initMutex();
}

//------------------------------------------------------------------------------
// determineMaxMsgLen
//------------------------------------------------------------------------------
size_t castor::log::Log::determineMaxMsgLen() const throw() {
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
std::map<int, std::string> castor::log::Log::generatePriorityToTextMap() const 
  throw(castor::exception::Internal) {
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
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to generate priority to text mapping: " <<
      se.what();
    throw ex;
  }

  return m;
}

//------------------------------------------------------------------------------
// checkProgramNameLen
//------------------------------------------------------------------------------
void castor::log::Log::checkProgramNameLen(const std::string &programName)
  throw(castor::exception::InvalidArgument) {
  if(programName.length() > LOG_MAX_PROGNAMELEN) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Invalid argument: programName is too log: max=" <<
      LOG_MAX_PROGNAMELEN << " actual=" << programName.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// initMutex
//------------------------------------------------------------------------------
void castor::log::Log::initMutex() throw(castor::exception::Internal) {
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if(0 != rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to initialize mutex attribute: " << sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
  if(0 != rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to set mutex type: " << sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutex_init(&m_mutex, NULL);
  if(0 != rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to initialize m_mutex: " << sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutexattr_destroy(&attr);
  if(0 != rc) {
    pthread_mutex_destroy(&m_mutex);
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to destroy mutex attribute: " << sstrerror(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::log::Log::~Log() throw() {
}

//------------------------------------------------------------------------------
// openLog
//------------------------------------------------------------------------------
void castor::log::Log::openLog() throw() {
  if(-1 == m_logFile) {
    struct sockaddr_un syslogAddr;
    syslogAddr.sun_family = AF_UNIX;
    strncpy(syslogAddr.sun_path, _PATH_LOG, sizeof(syslogAddr.sun_path));
    m_logFile = socket(AF_UNIX, SOCK_DGRAM, 0);
    if(-1 == m_logFile) {
      return;
    }

    if(-1 == fcntl(m_logFile, F_SETFD, FD_CLOEXEC)) {
      close(m_logFile);
      m_logFile = -1;
      return;
    }
  }
  if(!m_connected) {
    struct sockaddr_un syslogAddr;
    syslogAddr.sun_family = AF_UNIX;
    strncpy(syslogAddr.sun_path, _PATH_LOG, sizeof(syslogAddr.sun_path));
    if(-1 == connect(m_logFile, (struct sockaddr *)&syslogAddr,
      sizeof(syslogAddr))) {
      close(m_logFile);
      m_logFile = -1;
    } else {
#ifdef __APPLE__
      // MAC has has no MSG_NOSIGNAL
      // but >= 10.2 comes with SO_NOSIGPIPE
      int set = 1;
      if(0 != setsockopt(s_logFile, SOL_SOCKET, SO_NOSIGPIPE, &set,
        sizeof(int))) {
        close(m_logFile);
        m_logFile = -1;
        return;
      }
#endif
      m_connected = true;
    }
  }
}

//------------------------------------------------------------------------------
// closeLog
//------------------------------------------------------------------------------
void castor::log::Log::closeLog() throw() {
  if(!m_connected) {
    return;
  }
  close(m_logFile);
  m_logFile = -1;
  m_connected = false;
}

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::Log::writeMsg(
  const int priority,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[],
  const struct timeval &timeStamp) throw() {
  //---------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by building
  // the message ourselves. We then only call a reduced version of syslog
  // (namely reducedSyslog). The reason behind it is to be able to set the
  // message timestamp ourselves, in case we log messages asynchronously, as
  // we do when retrieving logs from the DB
  //----------------------------------------------------------------------------

  // Try to find the textual representation of the syslog priority
  std::map<int, std::string>::const_iterator priorityTextPair =
    m_priorityToText.find(priority);

  // Do nothing if the log priority is not valid
  if(m_priorityToText.end() == priorityTextPair) {
    return;
  }

  // Safe to get a reference to the textual representation of the priority
  const std::string &priorityText = priorityTextPair->second;

  // Initialize buffer
  char buffer[LOG_MAX_LINELEN * 2];
  memset(buffer,  '\0', sizeof(buffer));

  // Start message with priority, time, program and PID (syslog standard
  // format)
  size_t len = buildSyslogHeader(buffer, m_maxMsgLen, priority | LOG_LOCAL3,
    timeStamp, getpid());

  // Append the log level, the thread id and the message text
#ifdef __APPLE__
  len += snprintf(buffer + len, m_maxMsgLen - len, "LVL=%s TID=%d MSG=\"%s\" ",
    priorityText.c_str(),(int)mach_thread_self(), msg.c_str());
#else
  len += snprintf(buffer + len, m_maxMsgLen - len, "LVL=%s TID=%d MSG=\"%s\" ",
    priorityText.c_str(),(int)syscall(__NR_gettid), msg.c_str());
#endif

  // Process parameters
  for(int i = 0; i < numParams; i++) {
    const Param &param = params[i];

    // Check the parameter name, if it's an empty string set the value to
    // "Undefined".
    const std::string name = param.getName() == "" ? "Undefined" :
      cleanString(param.getName(), true);

    // Process the data type associated with the parameter
    switch(params[i].getType()) {
    // Strings
    case LOG_MSG_PARAM_TPVID:
    case LOG_MSG_PARAM_STR:
      {
        const std::string value = cleanString(param.getStrValue(), false);
        if(LOG_MSG_PARAM_TPVID == param.getType()) {
          len += snprintf(buffer + len, m_maxMsgLen - len, "TPVID=%.*s ",
            CA_MAXVIDLEN, value.c_str());
        } else {
          len += snprintf(buffer + len, m_maxMsgLen - len, "%.*s=\"%.*s\" ",
            (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
            (int)LOG_MAX_PARAMSTRLEN, value.c_str());
        }
      }
      break;
    // Numerical values
    case LOG_MSG_PARAM_INT:
      len += snprintf(buffer + len, m_maxMsgLen - len, "%.*s=%d ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getIntValue());
      break;
    case LOG_MSG_PARAM_INT64:
      len += snprintf(buffer + len, m_maxMsgLen - len, "%.*s=%lld ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getUint64Value());
      break;
    case LOG_MSG_PARAM_DOUBLE:
      len += snprintf(buffer + len, m_maxMsgLen - len, "%.*s=%f ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getDoubleValue());
      break;

    // Subrequest uuid
    case LOG_MSG_PARAM_UUID:
      {
        char uuidstr[CUUID_STRING_LEN + 1];
        if(Cuuid2string(uuidstr, CUUID_STRING_LEN + 1,
          &param.getUuidValue())) {
          return;
        }

        len += snprintf(buffer + len, m_maxMsgLen - len, "SUBREQID=%.*s ",
                      CUUID_STRING_LEN, uuidstr);
      }
      break;

    case LOG_MSG_PARAM_RAW:
      len += snprintf(buffer + len, m_maxMsgLen - len, "%s ",
        param.getStrValue().c_str());
      break;

    default:
      // Please note that this case is used for normal program execution
      // for the following parameter types:
      //
      //   LOG_MSG_PARAM_UID
      //   LOG_MSG_PARAM_GID
      //   LOG_MSG_PARAM_STYPE
      //   LOG_MSG_PARAM_SNAME
      break; // Nothing
    }

    // Check if there is enough space in the buffer
    if(len >= m_maxMsgLen) {
      buffer[m_maxMsgLen - 1] = '\n';
      break;
    }
  }

  // Terminate the string
  if(len < m_maxMsgLen) {
    len += snprintf(buffer + (len - 1), m_maxMsgLen - len, "\n");
  }

  reducedSyslog(buffer, len);
}

//-----------------------------------------------------------------------------
// buildSyslogHeader
//-----------------------------------------------------------------------------
int castor::log::Log::buildSyslogHeader(
  char *const buffer,
  const int buflen,
  const int priority,
  const struct timeval &timeStamp,
  const int pid) const throw() {
  struct tm tmp;
  int len = snprintf(buffer, buflen, "<%d>", priority);
  localtime_r(&(timeStamp.tv_sec), &tmp);
  len += strftime(buffer + len, buflen - len, "%Y-%m-%dT%T", &tmp);
  len += snprintf(buffer + len, buflen - len, ".%06ld",
    (unsigned long)timeStamp.tv_usec);
  len += strftime(buffer + len, buflen - len, "%z: ", &tmp);
  // dirty trick to have the proper timezone format (':' between hh and mm)
  buffer[len-2] = buffer[len-3];
  buffer[len-3] = buffer[len-4];
  buffer[len-4] = ':';
  // if no source given, you by default the name of the process in which we run
  // print source and pid
  len += snprintf(buffer + len, buflen - len, "%s[%d]: ",
    m_programName.c_str(), pid);
  return len;
}

//-----------------------------------------------------------------------------
// cleanString
//-----------------------------------------------------------------------------
std::string castor::log::Log::cleanString(const std::string &s,
  const bool replaceSpaces) throw() {
  char *str = strdup(s.c_str());

  // Return an empty string if the strdup() failed
  if(NULL == str) {
    return "";
  }

  // Variables
  char *end = NULL;
  char *ptr = NULL;

  // Replace newline and tab with a space
  while (((ptr = strchr(str, '\n')) != NULL) ||
         ((ptr = strchr(str, '\t')) != NULL)) {
    *ptr = ' ';
  }

  // Remove leading whitespace
  while (isspace(*str)) str++;

  // Remove trailing whitespace
  end = str + strlen(str) - 1;
  while (end > str && isspace(*end)) end--;

  // Write new null terminator
  *(end + 1) = '\0';

  // Replace double quotes with single quotes
  while ((ptr = strchr(str, '"')) != NULL) {
    *ptr = '\'';
  }

  // Check for replacement of spaces with underscores
  if (replaceSpaces) {
    while ((ptr = strchr(str, ' ')) != NULL) {
      *ptr = '_';
    }
  }

  std::string result(str);
  free(str);
  return result;
}

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void castor::log::Log::reducedSyslog(const char *const msg, const int msgLen)
  throw() {
  int send_flags = 0;
#ifndef __APPLE__
  // MAC has has no MSG_NOSIGNAL
  // but >= 10.2 comes with SO_NOSIGPIPE
  send_flags = MSG_NOSIGNAL;
#endif
  // enter critical section
  pthread_mutex_lock(&m_mutex);

  // Get connected, output the message to the local logger.
  if (!m_connected) {
    openLog();
  }

  if (!m_connected ||
    send(m_logFile, msg, msgLen, send_flags) < 0) {
    if (m_connected) {
      // Try to reopen the syslog connection.  Maybe it went down.
      closeLog();
      openLog();
    }
    if (!m_connected ||
      send(m_logFile, msg, msgLen, send_flags) < 0) {
      closeLog();  // attempt re-open next time
    }
  }

  // Leave critical section.
  pthread_mutex_unlock(&m_mutex);
}

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::Log::writeMsg(
  const int severity,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[]) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  writeMsg(severity, msg, numParams, params, timeStamp);
}
