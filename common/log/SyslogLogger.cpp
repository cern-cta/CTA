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
#include "common/utils/utils.hpp"

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
  Logger(programName, logMask),
  m_socketName(socketName),
  m_logFile(-1) {
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
// reducedSyslog
//-----------------------------------------------------------------------------
void cta::log::SyslogLogger::reducedSyslog(const std::string& msg) {
  // Truncate the log message if it exceeds the permitted maximum
  std::string truncatedMsg = msg.substr(0, m_maxMsgLen);

  int send_flags = MSG_NOSIGNAL;

  // enter critical section
  threading::MutexLocker lock(m_mutex);

  // Try to connect if not already connected
  openLog();

  // If connected
  if(-1 != m_logFile) {
    // If sending the log message fails then try to reopen the syslog
    // connection and try again
    if(0 > send(m_logFile, truncatedMsg.c_str(), truncatedMsg.length(), send_flags)) {
      closeLog();
      openLog();
      if (-1 != m_logFile) {
        // If the second attempt to send the log message fails then give up and
        // attempt re-open next time
        if(0 > send(m_logFile, truncatedMsg.c_str(), truncatedMsg.length(), send_flags)) {
          closeLog();
        }
      }
    }
  }
}
