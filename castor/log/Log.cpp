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
 * @(#)Dlf.cpp,v 1.1 $Release$ 2005/04/05 11:51:33 sponcec3
 *
 * Interface to the CASTOR logging system
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/log/Log.hpp"

#include <pthread.h>
#include <string.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

namespace castor {
namespace log {

/**
 * The file descriptor of the log.
 */
static int s_logFile = -1;

/**
 * AF_UNIX address of local logger.
 */
static struct sockaddr_un s_syslogAddr;

/**
 * True if the the syslog connect has bveen done.
 */
static bool s_connected = false;

/**
 * Mutex used to synchronize syslog client.
 */
static pthread_mutex_t s_syslogMutex;

//-----------------------------------------------------------------------------
// castor_openlog
//-----------------------------------------------------------------------------
static void castor_openlog() {
  if(-1 == s_logFile) {
    s_syslogAddr.sun_family = AF_UNIX;
    strncpy(s_syslogAddr.sun_path, _PATH_LOG, sizeof(s_syslogAddr.sun_path));
    s_logFile = socket(AF_UNIX, SOCK_DGRAM, 0);
    fcntl(s_logFile, F_SETFD, FD_CLOEXEC);
    if(-1 == s_logFile) {
      return;
    }
  }
  if(!s_connected) {
    if(-1 == connect(s_logFile, (struct sockaddr *)&s_syslogAddr,
      sizeof(s_syslogAddr))) {
      close(s_logFile);
      s_logFile = -1;
    } else {
#ifdef __APPLE__
      // MAC has has no MSG_NOSIGNAL
      // but >= 10.2 comes with SO_NOSIGPIPE
      int set = 1;
      if (0 != setsockopt(s_logFile, SOL_SOCKET, SO_NOSIGPIPE, &set,
        sizeof(int))) {
        close(s_logFile);
        s_logFile = -1;
        return;
      }
#endif
      s_connected = true;
    }
  }
}

//-----------------------------------------------------------------------------
// castor_closelog
//-----------------------------------------------------------------------------
static void castor_closelog() {
  if (!s_connected) return;
  close (s_logFile);
  s_logFile = -1;
  s_connected = false;
}

//-----------------------------------------------------------------------------
// castor_syslog
//-----------------------------------------------------------------------------
static void castor_syslog(const std::string &msg) throw() {
  int send_flags = 0;
#ifndef __APPLE__
  // MAC has has no MSG_NOSIGNAL
  // but >= 10.2 comes with SO_NOSIGPIPE
  send_flags = MSG_NOSIGNAL;
#endif
  // enter critical section
  pthread_mutex_lock(&s_syslogMutex);

  // Get connected, output the message to the local logger.
  if (!s_connected) {
    castor_openlog();
  }

  if (!s_connected ||
    send(s_logFile, msg.c_str(), msg.length(), send_flags) < 0) {
    if (s_connected) {
      // Try to reopen the syslog connection.  Maybe it went down.
      castor_closelog();
      castor_openlog();
    }
    if (!s_connected ||
      send(s_logFile, msg.c_str(), msg.length(), send_flags) < 0) {
      castor_closelog();  // attempt re-open next time
    }
  }

  // End of critical section.
  pthread_mutex_unlock(&s_syslogMutex);
}

} // namespace log
} // namespace castor

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::writeMsg(
  const int severity,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[],
  const struct timeval &timeStamp) throw() {
  //---------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by building
  // the message ourselves. We then only call a reduced version of syslog
  // (namely dlf_syslog). The reason behind it is to be able to set the
  // message timestamp ourselves, in case we log messages asynchronously, as
  // we do when retrieving logs from the DB
  //----------------------------------------------------------------------------

  const std::string toBeDoneMsg = "TO BE DONE";
  castor_syslog(toBeDoneMsg);
}

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::writeMsg(
  const int severity,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[]) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  writeMsg(severity, msg, numParams, params, timeStamp);
}
