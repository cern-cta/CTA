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
#include "h/Castor_limits.h"
#include "h/getconfent.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

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
static size_t LOG_MAX_PARAMSTRLEN = 1024;

/**
 * Maximum length of an ident/facility.
 */
static const size_t LOG_MAX_IDENTLEN = 20;

/**
 * Maximum length of a log message.
 */
static size_t LOG_MAX_LINELEN = 8192;

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

static struct {
  const char *name; // Name of the priority
  int  value;       // The priority's numeric representation in syslog
  const char *text; // Textual representation of the priority
} s_prioritylist[] = {
  { "LOG_EMERG",   LOG_EMERG,   "Emerg"  },
  { "LOG_ALERT",   LOG_ALERT,   "Alert"  },
  { "LOG_CRIT",    LOG_CRIT,    "Crit"   },
  { "LOG_ERR",     LOG_ERR,     "Error"  },
  { "LOG_WARNING", LOG_WARNING, "Warn"   },
  { "LOG_NOTICE",  LOG_NOTICE,  "Notice" },
  { "LOG_INFO",    LOG_INFO,    "Info"   },
  { "LOG_DEBUG",   LOG_DEBUG,   "Debug"  },
  { NULL,          0,           NULL     }
};

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
  if (strlen(ident) > LOG_MAX_IDENTLEN) {
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
  if ((size >= DEFAULT_SYSLOG_MSGLEN) && (size <= LOG_MAX_LINELEN)) {
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
static void castor_syslog(const char *const msg, const int msgLen) throw() {
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
    send(s_logFile, msg, msgLen, send_flags) < 0) {
    if (s_connected) {
      // Try to reopen the syslog connection.  Maybe it went down.
      castor_closelog();
      castor_openlog();
    }
    if (!s_connected ||
      send(s_logFile, msg, msgLen, send_flags) < 0) {
      castor_closelog();  // attempt re-open next time
    }
  }

  // End of critical section.
  pthread_mutex_unlock(&s_syslogMutex);
}

//-----------------------------------------------------------------------------
// build_syslog_header
//-----------------------------------------------------------------------------
static int build_syslog_header(char *const buffer,
                               const int buflen,
                               const int priority,
                               const struct timeval &timeStamp,
                               const int pid) {
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
  len += snprintf(buffer + len, buflen - len, "%s[%d]: ", s_progname, pid);
  return len;
}

//-----------------------------------------------------------------------------
// _clean_string
//-----------------------------------------------------------------------------
static std::string _clean_string(const std::string &s, const bool underscore) {
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
  if (underscore) {
    while ((ptr = strchr(str, ' ')) != NULL) {
      *ptr = '_';
    }
  }

  std::string result(str);
  free(str);
  return result;
}

} // namespace log
} // namespace castor

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::writeMsg(
  const int priority,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[],
  const struct timeval &timeStamp) throw() {
  //---------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by building
  // the message ourselves. We then only call a reduced version of syslog
  // (namely castor_syslog). The reason behind it is to be able to set the
  // message timestamp ourselves, in case we log messages asynchronously, as
  // we do when retrieving logs from the DB
  //----------------------------------------------------------------------------

  char   buffer[LOG_MAX_LINELEN * 2];
  size_t len = 0;

  // Do nothing if the logging interface has not been initialized
  if (!s_initialized) {
    return;
  }

  // Do nothing if the log priority is not valid
  if (priority > LOG_DEBUG) {
    return;
  }

  // Initialize buffers
  memset(buffer,  '\0', sizeof(buffer));

  // Start message with priority, time, program and PID (syslog standard
  // format)
  len += build_syslog_header(buffer, s_maxmsglen - len, priority | LOG_LOCAL3,
    timeStamp, getpid());

  // Append the log level, the thread id and the message text
#ifdef __APPLE__
  len += snprintf(buffer + len, s_maxmsglen - len, "LVL=%s TID=%d MSG=\"%s\" ",
    s_prioritylist[priority].text,(int)mach_thread_self(), msg.c_str());
#else
  len += snprintf(buffer + len, s_maxmsglen - len, "LVL=%s TID=%d MSG=\"%s\" ",
    s_prioritylist[priority].text,(int)syscall(__NR_gettid), msg.c_str());
#endif

  // Process parameters
  for (int i = 0; i < numParams; i++) {
    const Param &param = params[i];

    // Check the parameter name, if it's an empty string set the value to
    // "Undefined".
    const std::string name = param.getName() == "" ? "Undefined" :
      _clean_string(param.getName(), true);

    // Process the data type associated with the parameter
    switch (params[i].getType()) {
    // Strings
    case LOG_MSG_PARAM_TPVID:
    case LOG_MSG_PARAM_STR:
      {
        const std::string value = _clean_string(param.getStrValue(), false);
        if (LOG_MSG_PARAM_TPVID == param.getType()) {
          len += snprintf(buffer + len, s_maxmsglen - len, "TPVID=%.*s ",
            CA_MAXVIDLEN, value.c_str());
        } else {
          len += snprintf(buffer + len, s_maxmsglen - len, "%.*s=\"%.*s\" ",
            (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
            (int)LOG_MAX_PARAMSTRLEN, value.c_str());
        }
      }
      break;
    // Numerical values
    case LOG_MSG_PARAM_INT:
      len += snprintf(buffer + len, s_maxmsglen - len, "%.*s=%d ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getIntValue());
      break;
    case LOG_MSG_PARAM_INT64:
      len += snprintf(buffer + len, s_maxmsglen - len, "%.*s=%lld ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getUint64Value());
      break;
    case LOG_MSG_PARAM_DOUBLE:
      len += snprintf(buffer + len, s_maxmsglen - len, "%.*s=%f ",
        (int)LOG_MAX_PARAMNAMELEN, name.c_str(),
        param.getDoubleValue());
      break;

    // Subrequest uuid
    case LOG_MSG_PARAM_UUID:
      {
        char uuidstr[CUUID_STRING_LEN + 1];
        if (Cuuid2string(uuidstr, CUUID_STRING_LEN + 1,
          &param.getUuidValue())) {
          return;
        }

        len += snprintf(buffer + len, s_maxmsglen - len, "SUBREQID=%.*s ",
                      CUUID_STRING_LEN, uuidstr);
      }
      break;

    case LOG_MSG_PARAM_RAW:
      len += snprintf(buffer + len, s_maxmsglen - len, "%s ",
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
    if ((int)len >= s_maxmsglen) {
      buffer[s_maxmsglen - 1] = '\n';
      break;
    }
  }

  // Terminate the string
  if ((int)len < s_maxmsglen) {
    len += snprintf(buffer + (len - 1), s_maxmsglen - len, "\n");
  }

  castor_syslog(buffer, len);
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
