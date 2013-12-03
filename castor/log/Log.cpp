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
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/log/Log.hpp"
#include "h/getconfent.h"

#include <errno.h>
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
 * Default size of a syslog message.
 */
static const size_t DEFAULT_SYSLOG_MSGLEN = 1024;

/**
 * Default size of a rsyslog message.
 */
static const size_t DEFAULT_RSYSLOG_MSGLEN = 2000;

/**
 * Maximum length of a parameter name.
 */
static const size_t LOG_MAX_PARAMNAMELEN = 20;

/**
 * Maximum length of a string value.
 */
//static size_t LOG_MAX_PARAMSTRLEN = 1024;

/**
 * Maximum length of an ident/facility.
 */
static const size_t CASTOR_MAX_IDENTLEN = 20;

/**
 * Maximum length of a log message.
 */
static size_t CASTOR_MAX_LINELEN = 8192;

/**
 * True if the interface of the CASTOR logging system has been initialized.
 */
static bool s_initialized = false;

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

/**
 * ident/facility.
 */
static const char* s_progname = 0;

/**
 * The maximum message size that the client syslog server can handle.
 */
static int s_maxmsglen = DEFAULT_SYSLOG_MSGLEN;

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
// initLog
//-----------------------------------------------------------------------------
int initLog(const char *ident) {

  /* Variables */
  FILE *fp = NULL;
  char *p;
  char buffer[1024];
  size_t size = 0;

  /* Check if already initialized */
  if (s_initialized) {
    errno = EPERM;
    return (-1);
  }

  /* Check if the ident is too big */
  if (strlen(ident) > CASTOR_MAX_IDENTLEN) {
    errno = EINVAL;
    return (-1);
  }

  s_progname = ident;

  /* Determine the maximum message size that the client syslog server can
   * handle.
   */
  if ((p = getconfent("LOG", "MaxMessageSize", 0)) != NULL) {
    size = atoi(p);
  } else {
    /* Determine the size automatically, this is not guaranteed to work! */
    fp = fopen("/etc/rsyslog.conf", "r");
    if (fp) {
      /* The /etc/rsyslog.conf file exists so we assume the default message
       * size of 2K.
       */
      s_maxmsglen = DEFAULT_RSYSLOG_MSGLEN;

      /* In rsyslog versions >= 3.21.4, the maximum size of a message became
       * configurable through the $MaxMessageSize global config directive.
       * Here we attempt to find out if the user has increased the size!
       */
      while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        if (strncasecmp(buffer, "$MaxMessageSize", 15)) {
          continue; /* Option not of interest */
        }
        size = atol(&buffer[15]);
      }
      fclose(fp);
    } else {
      /* The /etc/rsyslog.conf file is missing which implies that we are
       * running on a stock syslogd system, therefore the message size is
       * governed by the syslog RFC: http://www.faqs.org/rfcs/rfc3164.html
       */
    }
  }

  /* Check that the size of messages falls within acceptable limits */
  if ((size >= DEFAULT_SYSLOG_MSGLEN) &&
      (size <= CASTOR_MAX_LINELEN)) {
    s_maxmsglen = size;
  }

  /* create the syslog serialization lock */
  if (pthread_mutex_init(&s_syslogMutex, NULL)) {
    errno = ENOMEM;
    return (-1);
  }

  /* Open syslog */
  castor_openlog();
  s_initialized = 1;

  return (0);
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
