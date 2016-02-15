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

#include "common/log/StringLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/threading/MutexLocker.hpp"

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
cta::log::StringLogger::StringLogger(const std::string &programName,
  const int logMask):
  Logger(programName),
  m_logMask(logMask),
  m_maxMsgLen(SyslogLogger::determineMaxMsgLen()),
  m_priorityToText(SyslogLogger::generatePriorityToTextMap()) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::log::StringLogger::~StringLogger() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::StringLogger::prepareForFork() {
}

//-----------------------------------------------------------------------------
// operator() 
//-----------------------------------------------------------------------------
void cta::log::StringLogger::operator() (
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

  SyslogLogger::writeLogMsg(
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
// reducedSyslog
//-----------------------------------------------------------------------------
void cta::log::StringLogger::reducedSyslog(std::string msg) {
  // Truncate the log message if it exceeds the permitted maximum
  if(msg.length() > m_maxMsgLen) {
    msg.resize(m_maxMsgLen);
    msg[msg.length() - 1] = '\n';
  }

  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Append the message to the log
  m_log << msg << std::endl;

  // Temporary hack: also print them out:
  printf (msg.c_str());
}
