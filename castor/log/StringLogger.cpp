/******************************************************************************
 *                      castor/log/StringLogger.cpp
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
 * Interface to the CASTOR logging system, with string output.
 *
 * @author Steven.Murray@cern.ch Eric.Cano@cern.ch
 *****************************************************************************/

#include "castor/log/StringLogger.hpp"
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
castor::log::StringLogger::StringLogger(
  const std::string &programName)
  throw(castor::exception::Internal, castor::exception::InvalidArgument):
  Logger(programName),
  m_maxMsgLen(determineMaxMsgLen()),
  m_priorityToText(generatePriorityToTextMap()) {
  initMutex();
}

//------------------------------------------------------------------------------
// determineMaxMsgLen
//------------------------------------------------------------------------------
size_t castor::log::StringLogger::determineMaxMsgLen() const throw() {
  const char *p = NULL;
  size_t msgSize = 0;

  if((p = getconfent("LOG", "MaxMessageSize", 0)) != NULL) {
    msgSize = atoi(p);
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
  castor::log::StringLogger::generatePriorityToTextMap() const 
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
// initMutex
//------------------------------------------------------------------------------
void castor::log::StringLogger::initMutex()
  throw(castor::exception::Internal) {
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if(0 != rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to initialize mutex attribute for m_mutex: " <<
      sstrerror(rc);
    throw ex;
  }
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
  if(0 != rc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to set mutex type of m_mutex: " <<
      sstrerror(rc);
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
    ex.getMessage() << "Failed to destroy mutex attribute of m_mutex: " <<
      sstrerror(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::log::StringLogger::~StringLogger() throw() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void castor::log::StringLogger::prepareForFork() 
  throw(castor::exception::Internal) {}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::StringLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::vector<Param> &params,
  const struct timeval &timeStamp) throw() {
  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::StringLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params,
  const struct timeval &timeStamp) throw() {
  operator() (priority, msg, params.begin(), params.end(), timeStamp);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::StringLogger::operator() (
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
std::string castor::log::StringLogger::buildSyslogHeader(
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
std::string castor::log::StringLogger::cleanString(const std::string &s,
  const bool replaceSpaces) throw() {

  //find first non white char
  const std::string& spaces="\t\n\v\f\r ";
  size_t beginpos = s.find_first_not_of(spaces);
  std::string::const_iterator it1;
  if (std::string::npos != beginpos)
    it1 = beginpos + s.begin();
  else
    it1 = s.begin();
  
  //find last non white char
  std::string::const_iterator it2;
  size_t endpos = s.find_last_not_of(spaces);
  if (std::string::npos != endpos) 
    it2 = endpos + 1 + s.begin();
  else 
    it2 = s.end();
  
  std::string result(it1, it2);
  
//  if (s.begin() == it1 && it2 == s.end()) 
//    result="";
  
  for (std::string::iterator it = result.begin(); it != result.end(); ++it) {

    // Replace newline and tab with a space
    if (replaceSpaces) {
      if ('\t' == *it) 
        *it = ' ';
      
      if ('\n' == *it) 
        *it = ' ';
    }
    
    // Replace spaces with underscore
    if (' ' == *it) 
      *it = '_';
    
    // Replace double quotes with single quotes
    if ('"' == *it) 
      *it = '\'';
  }
  return result;
   
}

//-----------------------------------------------------------------------------
// reducedSyslog
//-----------------------------------------------------------------------------
void castor::log::StringLogger::reducedSyslog(std::string msg)
  throw() {
  // Truncate the log message if it exceeds the permitted maximum
  if(msg.length() > m_maxMsgLen) {
    msg.resize(m_maxMsgLen);
    msg[msg.length() - 1] = '\n';
  }

  // enter critical section
  const int mutex_lock_rc = pthread_mutex_lock(&m_mutex);
  // Do nothing if we failed to enter the critical section
  if(0 != mutex_lock_rc) {
    return;
  }

  // Append the message to the log
  m_log << msg << std::endl;
  
  // Temporary hack: also print them out:
  printf (msg.c_str());

  // Leave critical section.
  pthread_mutex_unlock(&m_mutex);
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void castor::log::StringLogger::operator() (
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
void castor::log::StringLogger::operator() (
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
void castor::log::StringLogger::operator() (
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
void castor::log::StringLogger::operator() (
  const int priority,
  const std::string &msg) throw() {

  Param *emptyParams = NULL;
  operator() (priority, msg, 0, emptyParams);
}
