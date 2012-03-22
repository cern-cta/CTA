/******************************************************************************
 *                      dlf/dlf_lib.c
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
 * @(#)$RCSfile: dlf_lib.c,v $ $Revision: 1.33 $ $Release$ $Date: 2009/08/18 09:42:58 $ $Author: waldron $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Include files */
#include "dlf_lib.h"
#include "dlf_api.h"
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#ifdef __APPLE__
#include <mach/mach.h>
#endif

/* Global variables */
static char *messages[DLF_MAX_MSGTEXTS];
static int   initialized = 0;
static int   maxmsglen   = DEFAULT_SYSLOG_MSGLEN;
static int   logmask     = 0xff;
static const char* progname = 0;
static int   LogFile = -1;           /* fd for log */
static int   connected;              /* have done connect */
static pthread_mutex_t syslog_lock;  /* synchronization of syslog clients */
static pthread_key_t  reg_key;
static pthread_once_t reg_key_once = PTHREAD_ONCE_INIT;
static struct sockaddr_un SyslogAddr;	/* AF_UNIX address of local logger */

/* local functions */
static void dlf_openlog();
static void dlf_closelog();
static void dlf_syslog(char* msg, int msglen);

/*---------------------------------------------------------------------------
 * _tls_reg_key_alloc & _tls_req_data_free
 *---------------------------------------------------------------------------*/
void _tls_req_data_free(void *data) {
  free(data);
}

static void _tls_reg_key_alloc() {
  pthread_key_create(&reg_key, &_tls_req_data_free);
}


/*---------------------------------------------------------------------------
 * _clean_string
 *---------------------------------------------------------------------------*/
char *_clean_string(char *str, int underscore) {

  /* Variables */
  char *end = NULL;
  char *ptr = NULL;

  /* Replace newline and tab with a space */
  while (((ptr = strchr(str, '\n')) != NULL) ||
         ((ptr = strchr(str, '\t')) != NULL)) {
    *ptr = ' ';
  }

  /* Remove leading whitespace */
  while (isspace(*str)) str++;

  /* Remove trailing whitespace */
  end = str + strlen(str) - 1;
  while (end > str && isspace(*end)) end--;

  /* Write new null terminator */
  *(end + 1) = '\0';

  /* Replace double quotes with single quotes */
  while ((ptr = strchr(str, '"')) != NULL) {
    *ptr = '\'';
  }

  /* Check for replacement of spaces with underscores */
  if (underscore) {
    while ((ptr = strchr(str, ' ')) != NULL) {
      *ptr = '_';
    }
  }

  return str;
}


/*---------------------------------------------------------------------------
 * dlf_init
 *---------------------------------------------------------------------------*/
int dlf_init(const char *ident, int maskpri) {

  /* Variables */
  FILE *fp = NULL;
  char *p;
  char buffer[1024];
  int  i, found, size = 0;

  /* Check if already initialized */
  if (initialized) {
    errno = EPERM;
    return (-1);
  }

  /* Initialize the messages array. */
  for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
    messages[i] = NULL;
  }

  /* Check if the ident is too big */
  if (strlen(ident) > DLF_MAX_IDENTLEN) {
    errno = EINVAL;
    return (-1);
  }

  /* The default syslog log mask is set so that all messages irrespective of
   * there priority should be logged. This can be changed by modifying the
   * syslog configuration file but this is not formant as syslogd still
   * needs to process the message to determine whether or not it should be
   * logged. So, we set the syslog priority mask at the client application.
   */

  /* The default mask for castor is to logged everything up to and including
   * INFO messages. I.e. we exclude debug messages.
   */
  if (maskpri < 0) {
    (void)setlogmask(LOG_UPTO(LOG_INFO));

    /* Check if the configuration file defines the log mask to use */
    if ((p = getconfent(ident, "LogMask", 0)) != NULL) {
      /* Lookup the prority in the priority list */
      found = 0;
      for (i = 0; prioritylist[i].name != NULL; i++) {
        if (!strcasecmp(p, prioritylist[i].name)) {
          (void)setlogmask(LOG_UPTO(prioritylist[i].value));
          found = 1;
          break;
        }
      }
      /* If the priority wasn't found abort the initialization of the
       * logging interface
       */
      if (!found) {
        errno = EINVAL;
        return (-1);
      }
    }
  } else {
    /* Use the mask supplied by the user */
    (void)setlogmask(maskpri);
  }
  logmask = setlogmask(0);
  progname = ident;

  /* Determine the maximum message size that the client syslog server can
   * handle.
   */
  if ((p = getconfent("DLF", "MaxMessageSize", 0)) != NULL) {
    size = atoi(p);
  } else {
    /* Determine the size automatically, this is not guaranteed to work! */
    fp = fopen("/etc/rsyslog.conf", "r");
    if (fp) {
      /* The /etc/rsyslog.conf file exists so we assume the default message
       * size of 2K.
       */
      maxmsglen = DEFAULT_RSYSLOG_MSGLEN;

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
      (size <= DLF_MAX_LINELEN)) {
    maxmsglen = size;
  }

  /* Create the thread-specific data key */
  pthread_once(&reg_key_once, _tls_reg_key_alloc);
  /* create the syslog serialization lock */
  if (pthread_mutex_init(&syslog_lock, NULL)) {
    errno = ENOMEM;
    return (-1);
  }

  /* Open syslog */
  dlf_openlog();
  initialized = 1;

  return (0);
}


/*---------------------------------------------------------------------------
 * dlf_shutdwn
 *---------------------------------------------------------------------------*/
int dlf_shutdown(void) {

  /* Variables */
  int i;

  /* Logging interface is initialized? */
  if (!initialized) {
    errno = EPERM;
    return (-1);
  }

  /* Free the resources allocated to the messages array */
  for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
    if (messages[i] != NULL)
      free(messages[i]);
  }

  /* Close syslog */
  closelog();

  /* Delete the thread-specific data key */
  pthread_key_delete(reg_key);
  /* destroy the syslog serialization lock */
  pthread_mutex_destroy(&syslog_lock);

  /* Reset the defaults */
  maxmsglen   = DEFAULT_SYSLOG_MSGLEN;
  initialized = 0;

  return (0);
}


/*---------------------------------------------------------------------------
 * dlf_regtext
 *---------------------------------------------------------------------------*/
int dlf_regtext(unsigned int msgno, const char *message) {

  /* Logging interface is initialized? */
  if (!initialized) {
    errno = EPERM;
    return (-1);
  }

  /* Verify that the message number and associated text are valid */
  if (message == NULL) {
    errno = EINVAL;
    return (-1);
  }
  if (msgno > DLF_MAX_MSGTEXTS) {
    errno = EINVAL;
    return (-1);
  }
  if (strlen(message) > DLF_MAX_PARAMSTRLEN) {
    errno = EINVAL;
    return (-1);
  }

  /* Insert the message into the messages array */
  if (messages[msgno] != NULL) {
    free(messages[msgno]);
    messages[msgno] = NULL;
  }
  messages[msgno] = strdup(message);
  if (messages[msgno] == NULL) {
    return (-1);
  }

  return (0);
}


/*---------------------------------------------------------------------------
 * dlf_write
 *---------------------------------------------------------------------------*/
int dlf_write(Cuuid_t reqid,
              unsigned int priority,
              unsigned int msgno,
              struct Cns_fileid *ns,
              int numparams,
              ...) {

  /* Variables */
  dlf_write_param_t *params = NULL;
  va_list           ap;
  int               i;
  int               rv;
  char              *buf = NULL;

  /* Logging interface is initialized? */
  if (!initialized) {
    errno = EPERM;
    return (-1);
  }

  if (priority > LOG_DEBUG) {
    errno = EINVAL;
    return (-1);
  }

  /* Ignore messages whose priority is not of interest */
  if ((LOG_MASK(LOG_PRI(priority)) & logmask) == 0) {
    return (0);
  }

  params = (dlf_write_param_t *)
    malloc(numparams * sizeof(dlf_write_param_t));
  if (params == NULL) {
    return (-1);
  }

  /* Process parameters */
  va_start(ap, numparams);
  for (i = 0; i < numparams; i++) {

    /* Extract parameter name */
    params[i].name = NULL;
    if ((buf = va_arg(ap, char *)) != NULL) {
      params[i].name = strdup(buf);
    }

    /* Extract parameter data type */
    params[i].type = va_arg(ap, int);

    /* Process parameter data type */
    switch(params[i].type) {
    case DLF_MSG_PARAM_TPVID:
    case DLF_MSG_PARAM_STR:
      params[i].value.par_string = NULL;
      if ((buf = va_arg(ap, char *)) != NULL) {
        params[i].value.par_string = strdup(buf);
      }
      break;
    case DLF_MSG_PARAM_INT:
      params[i].value.par_int = va_arg(ap, int);
      break;
    case DLF_MSG_PARAM_INT64:
      params[i].value.par_u64 = va_arg(ap, signed64);
      break;
    case DLF_MSG_PARAM_DOUBLE:
      params[i].value.par_double = va_arg(ap, double);
      break;
    case DLF_MSG_PARAM_UUID:
      params[i].value.par_uuid = va_arg(ap, Cuuid_t);
      break;
    case DLF_MSG_PARAM_FLOAT:
      /* Floats are treated as doubles */
      params[i].value.par_double = va_arg(ap, double);
      params[i].type = DLF_MSG_PARAM_DOUBLE;
      break;
    default:
      /* Set unknown data types to strings with a NULL value. They will appear
       * in the log file with the parameter name="(null)" message to indicate
       * an error
       */
      params[i].value.par_string = NULL;
      params[i].type = DLF_MSG_PARAM_STR;
      break;
    }
  }
  va_end(ap);

  rv = dlf_writep(reqid, priority, msgno, ns, numparams, params);

  /* Free resources */
  for (i = 0; i < numparams; i++) {
    if (params[i].name != NULL)
      free(params[i].name);
    if ((params[i].type == DLF_MSG_PARAM_TPVID) ||
        (params[i].type == DLF_MSG_PARAM_STR)) {
      if (params[i].value.par_string != NULL)
        free(params[i].value.par_string);
    }
  }
  free(params);

  return (rv);
}

/*---------------------------------------------------------------------------
 * build_syslog_header
 *---------------------------------------------------------------------------*/
static int build_syslog_header(char *buffer,
                               int buflen,
                               int priority,
                               struct timeval *tv) {
  struct tm tmp;
  int len = snprintf(buffer, buflen, "<%d>", priority);
  localtime_r(&(tv->tv_sec), &tmp);
  len += strftime(buffer + len, buflen - len, "%Y-%m-%dT%T", &tmp);
  len += snprintf(buffer + len, buflen - len, ".%06ld", (unsigned long)tv->tv_usec);
  len += strftime(buffer + len, buflen - len, "%z: ", &tmp);
  // dirty trick to have the proper timezone format (':' between hh and mm)
  buffer[len-2] = buffer[len-3];
  buffer[len-3] = buffer[len-4];
  buffer[len-4] = ':';
  len += snprintf(buffer + len, buflen - len, "%s[%d]: ", progname, getpid());
  return len;
}

/*---------------------------------------------------------------------------
 * dlf_writep
 *---------------------------------------------------------------------------*/
int dlf_writep(Cuuid_t reqid,
               unsigned int priority,
               unsigned int msgno,
               struct Cns_fileid *ns,
               unsigned int numparams,
               dlf_write_param_t params[]) {
  char *msg;
  int deallocate = 0;
  // Verify that the msgno argument is valid and exists
  if (msgno > DLF_MAX_MSGTEXTS || messages[msgno] == NULL) {
    // invalid message number, give a static default message
    msg = calloc(40,1);
    if (0 == msg) {
      msg = "Unknown Error number and could not log number (out of memory)";
    } else {
      snprintf(msg, 40, "Unknown Error number %d", msgno);
      deallocate = 1;
    }
  } else {
    msg = messages[msgno];
    // If this is the first time the current thread has processed a message of type
    // msgno then record a registration message. (See #77741)
    // Deprecated : this can be dropped once the DLF DB is revisited
    {
      unsigned int *registered = pthread_getspecific(reg_key);
      if (registered == NULL) {
        unsigned int *tls = (unsigned int *)
          calloc(DLF_MAX_MSGTEXTS, sizeof(unsigned int));
        if (tls == NULL) {
          return (-1);
        }
        pthread_setspecific(reg_key, (void *)tls);
        registered = pthread_getspecific(reg_key);
      }

      /* Sanity check: This should never happen! */
      if (registered == NULL) {
        return (-1);
      }
      if (registered[msgno] == 0) {
        char tmpbuffer[DLF_MAX_LINELEN * 2];
        int tmpbuflen;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        tmpbuflen = build_syslog_header(tmpbuffer, DLF_MAX_LINELEN * 2, LOG_INFO | LOG_LOCAL2, &tv);
        tmpbuflen += snprintf(tmpbuffer + tmpbuflen, DLF_MAX_LINELEN * 2 - tmpbuflen,
                              "MSGNO=%u MSGTEXT=\"%s\"\n", msgno, msg);
        dlf_syslog(tmpbuffer, tmpbuflen);
        registered[msgno] = 1;
      }
    }
  }
  // and call actual logging method
  int ret = dlf_writepm(reqid, priority, msg, ns, numparams, params);
  if (deallocate) free(msg);
  return ret;
}

/*---------------------------------------------------------------------------
 * dlf_writepm
 *---------------------------------------------------------------------------*/
int dlf_writepm(Cuuid_t reqid,
                unsigned int priority,
                char* msg,
                struct Cns_fileid *ns,
                unsigned int numparams,
                dlf_write_param_t params[]) {
  // get the current time
  struct timeval tv;
  gettimeofday(&tv, NULL);
  // and call actual logging method
  return dlf_writept(reqid, priority, msg, ns, numparams, params, &tv);
}

/*---------------------------------------------------------------------------
 * dlf_writept
 * Note that we do here part of the work of the real syslog call, by building
 * the message ourselves. We then only call a reduced version of syslog
 * (namely dlf_syslog). The reason behind it is to be able to set the
 * message timestamp ourselves, in case we log messages asynchronously, as
 * we do when retrieving logs from the DB
 *---------------------------------------------------------------------------*/
int dlf_writept(Cuuid_t reqid,
                unsigned int priority,
                const char* msg,
                struct Cns_fileid *ns,
                unsigned int numparams,
                dlf_write_param_t params[],
                struct timeval *tv) {

  /* Variables */
  char   uuidstr[CUUID_STRING_LEN + 1];
  char   buffer[DLF_MAX_LINELEN * 2];
  char   *name = NULL;
  char   *value = NULL;
  char   *p = NULL;
  unsigned int    i;
  size_t len = 0;

  /* Logging interface is initialized? */
  if (!initialized) {
    errno = EPERM;
    return (-1);
  }

  /* Initialize buffers */
  memset(uuidstr, '\0', CUUID_STRING_LEN + 1);
  memset(buffer,  '\0', sizeof(buffer));

  if (priority > LOG_DEBUG) {
    errno = EINVAL;
    return (-1);
  }

  /* Ignore messages whose priority is not of interest */
  if ((LOG_MASK(LOG_PRI(priority)) & logmask) == 0) {
    return (0);
  }

  /* Convert the request uuid type to a string */
  if (Cuuid2string(uuidstr, CUUID_STRING_LEN + 1, &reqid)) {
    errno = EINVAL;
    return (-1);
  }

  /* start message with priority, time, program and PID (syslog standard format) */
  len += build_syslog_header(buffer, maxmsglen - len, priority | LOG_LOCAL3, tv);

  /* append the log level, the thread id and the message text */
#ifdef __APPLE__
  len += snprintf(buffer + len, maxmsglen - len, "LVL=%s TID=%d MSG=\"%s\" ",
                  prioritylist[priority].text,(int)mach_thread_self(), msg);
#else
  len += snprintf(buffer + len, maxmsglen - len, "LVL=%s TID=%d MSG=\"%s\" ",
                  prioritylist[priority].text,(int)syscall(__NR_gettid), msg);
#endif

  /* Append the request uuid if defined */
  if (Cuuid_compare(&reqid, &nullCuuid)) {
    len += snprintf(buffer + len, maxmsglen - len, "REQID=%s ", uuidstr);
  }

  /* Name server information */
  if (ns && ns->server[0]) {
    len += snprintf(buffer + len, maxmsglen - len,
                    "NSHOSTNAME=%.*s NSFILEID=%llu ",
                    CA_MAXHOSTNAMELEN, ns->server, ns->fileid);
  }

  /* Process parameters */
  for (i = 0; i < numparams; i++) {

    /* Check the parameter name, if it's NULL or an empty string set the value
     * to 'Undefined'.
     */
    if (params[i].type != DLF_MSG_PARAM_RAW) {
      p = params[i].name;
      if (!p || (p && !strcmp(p, ""))) {
        name = strdup("Undefined"); /* Default value */
      } else {
        name = strdup(p);
      }
      
      /* Check for memory allocation failure */
      if (name == NULL) {
        return (-1);
      }
    }

    /* Process the data type associated to the parameter */
    switch (params[i].type) {
      /* Strings */
    case DLF_MSG_PARAM_TPVID:
    case DLF_MSG_PARAM_STR:
      {
        /* Check the value */
        if ((p = params[i].value.par_string) == NULL) {
          value = strdup("(null)");  /* Default value */
        } else {
          value = strdup(p);
        }

        /* Check for memory allocation failure */
        if (value == NULL) {
          return (-1);
        }

        if (params[i].type == DLF_MSG_PARAM_TPVID) {
          len += snprintf(buffer + len, maxmsglen - len, "TPVID=%.*s ",
                          CA_MAXVIDLEN, _clean_string(value, 0));
        } else {
          len += snprintf(buffer + len, maxmsglen - len, "%.*s=\"%.*s\" ",
                          DLF_MAX_PARAMNAMELEN, _clean_string(name, 1),
                          DLF_MAX_PARAMSTRLEN, _clean_string(value, 0));
        }
        free(value);
        break;
      }

      /* Numerical values */
    case DLF_MSG_PARAM_INT:
      len += snprintf(buffer + len, maxmsglen - len, "%.*s=%d ",
                      DLF_MAX_PARAMNAMELEN, _clean_string(name, 1),
                      params[i].value.par_int);
      break;
    case DLF_MSG_PARAM_INT64:
      len += snprintf(buffer + len, maxmsglen - len, "%.*s=%lld ",
                      DLF_MAX_PARAMNAMELEN, _clean_string(name, 1),
                      params[i].value.par_u64);
      break;
    case DLF_MSG_PARAM_DOUBLE:
      len += snprintf(buffer + len, maxmsglen - len, "%.*s=%f ",
                      DLF_MAX_PARAMNAMELEN, _clean_string(name, 1),
                      params[i].value.par_double);
      break;

      /* Subrequest uuid */
    case DLF_MSG_PARAM_UUID:
      if (Cuuid2string(uuidstr, CUUID_STRING_LEN + 1,
                       &(params[i].value.par_uuid))) {
        free(name);
        errno = EINVAL;
        return (-1);
      }
      len += snprintf(buffer + len, maxmsglen - len, "SUBREQID=%.*s ",
                      CUUID_STRING_LEN, uuidstr);
      break;

    case DLF_MSG_PARAM_RAW:
      len += snprintf(buffer + len, maxmsglen - len, "%s ", params[i].value.par_string);
      break;

    default:
      break; /* Nothing */
    }
    free(name);

    /* Check if there is enough space in the buffer */
    if ((int)len >= maxmsglen) {
      buffer[maxmsglen - 1] = '\n';
      break;
    }
  }

  /* Terminate the string */
  if ((int)len < maxmsglen) {
    len += snprintf(buffer + (len - 1), maxmsglen - len, "\n");
  }

  dlf_syslog(buffer, len);

  return (0);
}

/*---------------------------------------------------------------------------
 * dlf_isinitialized
 *---------------------------------------------------------------------------*/
int dlf_isinitialized(void) {
  return (initialized);
}

/*---------------------------------------------------------------------------
 * dlf_openlog
 *---------------------------------------------------------------------------*/
static void dlf_openlog() {
  if (LogFile == -1) {
    SyslogAddr.sun_family = AF_UNIX;
    (void)strncpy(SyslogAddr.sun_path, _PATH_LOG,
                  sizeof(SyslogAddr.sun_path));
    LogFile = socket(AF_UNIX, SOCK_DGRAM, 0);
    fcntl(LogFile, F_SETFD, FD_CLOEXEC);
    if (LogFile == -1)
      return;
  }
  if (!connected) {
    if (connect(LogFile, (struct sockaddr *)&SyslogAddr, sizeof(SyslogAddr)) == -1) {
      (void)close(LogFile);
      LogFile = -1;
    } else
#ifdef __APPLE__
      // MAC has has no MSG_NOSIGNAL
      // but >= 10.2 comes with SO_NOSIGPIPE
      int set = 1;
    if (0 != setsockopt(LogFile, SOL_SOCKET, SO_NOSIGPIPE, &set, sizeof(int))) {
      (void)close(LogFile);
      LogFile = -1;
      return;
    }
#endif
    connected = 1;
  }
}

/*---------------------------------------------------------------------------
 * dlf_closelog
 *---------------------------------------------------------------------------*/
static void dlf_closelog(void) {
  if (!connected) return;
  close (LogFile);
  LogFile = -1;
  connected = 0;
}

/*---------------------------------------------------------------------------
 * dlf_syslog
 * this function is a simplified equivalent of syslog that directly deals
 * with the /dev/syslog device so that we can send our own messages and
 * especially play with the timestamp
 *---------------------------------------------------------------------------*/
static void dlf_syslog(char* msg, int msglen) {
  int send_flags = MSG_NOSIGNAL;
#ifdef __APPLE__
  // MAC has has no MSG_NOSIGNAL
  // but >= 10.2 comes with SO_NOSIGPIPE
  send_flags = 0;
#endif
  /* enter critical section */
  pthread_mutex_lock(&syslog_lock);

  /* Get connected, output the message to the local logger. */
  if (!connected) dlf_openlog();

  if (!connected || send(LogFile, msg, msglen, send_flags) < 0) {
    if (connected) {
      /* Try to reopen the syslog connection.  Maybe it went down.  */
      dlf_closelog ();
      dlf_openlog();
    }
    if (!connected || send(LogFile, msg, msglen, send_flags) < 0) {
      dlf_closelog ();	/* attempt re-open next time */
    }
  }

  /* End of critical section.  */
  pthread_mutex_unlock(&syslog_lock);
}

