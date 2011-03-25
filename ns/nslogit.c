/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "Cns.h"
#include "Cns_server.h"
#include "serrno.h"

/* Externs */
char nshostname[CA_MAXHOSTNAMELEN + 1];

/* Global variable */
static char *app_localhost = NULL;
static char  app_ident[30];
static char  app_logfile[CA_MAXPATHLEN + 1];
static int   app_init = 0;

int openlog(const char *ident, const char *logfile) {

  /* Variables */
  struct hostent *hent;
  char *buf      = NULL;
  char *p        = NULL;
  size_t len     = 128;
  unsigned int i = 0;

  /* Already initialized? */
  if (app_init == 1) {
    errno = EPERM;
    return 1;
  }

  /* Copy the program ident/tag to the global variable converting all
   * characters to lower case.
   */
  memset(app_ident, '\0', sizeof(app_ident));
  if (ident != NULL) {
    strncpy(app_ident, ident, sizeof(app_ident));
    app_ident[sizeof(app_ident)] = '\0';
    for (i = 0; i < strlen(app_ident); i++) {
      app_ident[i] = tolower(app_ident[i]);
    }
  }

  /* Copy the location of the logfile. */
  strncpy(app_logfile, logfile, sizeof(app_logfile));
  app_logfile[sizeof(app_logfile)] = '\0';

  /* Get the name of the localhost. IMHO, the logic to get the local hostname
   * is far more complex than it should be!
   */
  do {
    /* The buffer already exists which means that we encountered ENAMETOOLONG
     * and need to increase the buffer size. We have no indications of how
     * much to increase by so just double it!
     */
    if (buf != NULL) {
      len *= 2;
      free(buf);
    }
    if ((buf = (char *)malloc(len)) == NULL) {
      return 1;
    }
  } while ((gethostname(buf, len) == 0 && !memchr(buf, '\0', len)) ||
           errno == ENAMETOOLONG);

  /* The call to gethostname is not guaranteed to return a fully qualified
   * domain name. If the result is not fully qualified we call gethostbyname()
   * in the hope to determine it.
   */
  if ((p = strchr(buf, '.')) == NULL) {
    if ((hent = gethostbyname(buf)) != NULL) {
      free(buf);
      if ((buf = strdup(hent->h_name)) == NULL) {
        return 1;
      }
    } else {
      free(buf);
      return 1;
    }
  }

  /* Copy the answer to the global localhost variable. */
  if (buf) {
    app_localhost = strdup(buf);
    free(buf);
  }

  /* Hopefully, at this point we have a FQDN, this is not guaranteed though! We
   * continue anyway.
   */
  app_init = 1;
  return 0;
}

int closelog() {

  /* Logging interfaced initialized? */
  if (app_init == 0)
    return 0;

  /* Free resources allocated during the call to openlog(). */
  if (app_localhost != NULL)
    free(app_localhost);
  app_init = 0;

  return 0;
}

int _format_header(char *buffer, size_t buflen) {

  /* Variables */
  struct timeval tv;
  struct tm      *tm;
  struct tm      tm_buf;
  int            len;

  char offset_mode;
  char offset_hour;
  char offset_minute;

  /* Get current localtime */
  gettimeofday(&tv, NULL);
  tm = localtime_r((time_t *) &(tv.tv_sec), &tm_buf);

  /* Handle the timezone information. Note: The code to do this was essentially
   * taken from datetime.c of the rsyslog project.
   */
  offset_mode = '+';
  if (tm->tm_gmtoff < 0) {
    offset_mode = '-';
    offset_hour   = (tm->tm_gmtoff * -1) / 3600;
    offset_minute = (tm->tm_gmtoff * -1) % 3600;
  } else {
    offset_hour   = tm->tm_gmtoff / 3600;
    offset_minute = tm->tm_gmtoff % 3600;
  }

  /* Format the header of the message using the RSYSLOG_FileFormat type
   * RFC 3339 timestamps.
   */
  len = snprintf(buffer, buflen,
                 "%04d-%02d-%02dT%02d:%02d:%02d.%06u%c%02d:%02d "
                 "%s %s[%d]: LVL=Info TID=%d ",
                 tm->tm_year + 1900,
                 tm->tm_mon  + 1,
                 tm->tm_mday,
                 tm->tm_hour,
                 tm->tm_min,
                 tm->tm_sec,
                 (unsigned int)tv.tv_usec,
                 offset_mode,
                 offset_hour,
                 offset_minute,
                 app_localhost == NULL ? "127.0.0.1" : app_localhost,
                 app_ident[0]  == '\0' ? "unknown"   : app_ident,
                 getpid(),
                 (int)syscall(__NR_gettid));
  return len;
}

int _write_to_log(char *buffer, int buflen) {

  /* Variables */
  int fd;

  /* Add a newline '\n' character to the end of the message if not already
   * present.
   */
  if (buffer[buflen - 1] != '\n') {
    buffer[buflen - 1] = '\n';
  }

  /* Write the message to the log file. */
  fd = open(app_logfile, O_WRONLY | O_CREAT | O_APPEND, 0664);
  if (fd == -1) {
    return 1;
  }
  write(fd, buffer, buflen);
  close(fd);

  return 0;
}

int nslogreq(struct Cns_srv_request_info *reqinfo,
             const char *func,
             const int errorcode) {

  /* Variables */
  struct  timeval tv;
  char    buffer[LOGBUFSZ * 2]; // 4096 * 2
  double  elapsed;
  int     len;

  /* Logging interfaced initialized? */
  if (app_init == 0)
    return 0;

  /* Retrieve the header part of the message. */
  len = _format_header(buffer, sizeof(buffer));

  /* Here we format the message parameters in accordance with the DLF
   * specifications. First we handle the REQID, NSFILID and NSHOSTNAME. Note:
   * we do not check to see if the message has been truncated!
   */
  if (reqinfo->fileid) {
    len += snprintf(buffer + len, sizeof(buffer) - len,
                    "MSG=\"Processing complete\" REQID=%s "
                    "NSHOSTNAME=%.*s NSFILEID=%llu Function=\"%.255s\" ",
                    reqinfo->requuid, CA_MAXHOSTNAMELEN, nshostname,
                    reqinfo->fileid, func);
  } else {
    len += snprintf(buffer + len, sizeof(buffer) - len,
                    "MSG=\"Processing complete\" REQID=%s Function=\"%.255s\" ",
                    reqinfo->requuid, func);
  }

  /* Append the request credentials which are part of every message. */
  len += snprintf(buffer + len, sizeof(buffer) - len,
                  "Username=\"%.30s\" Uid=%d Gid=%d ClientHost=\"%.255s\" ",
                  reqinfo->username, reqinfo->uid, reqinfo->gid,
                  reqinfo->clienthost);

  /* Append the log buffer associated to the request. */
  if (reqinfo->logbuf[0] != '\0') {
    reqinfo->logbuf[sizeof(reqinfo->logbuf)] = '\0';
    len += snprintf(buffer + len, sizeof(buffer) - len, "%s ", reqinfo->logbuf);
  }

  /* Calculate the elapsed processing time. */
  gettimeofday(&tv, NULL);
  elapsed = (((((double)tv.tv_sec * 1000) +
               ((double)tv.tv_usec / 1000))) - reqinfo->starttime) * 0.001;

  /* Append the return code, error message if applicable and the elapsed
   * processing time of the request */
  if (errorcode) {
    len += snprintf(buffer + len, sizeof(buffer) - len,
                    "RtnCode=%d ErrorMessage=\"%s\" ElapsedTime=%.3f ",
                    errorcode, sstrerror(errorcode), elapsed);
  } else {
    len += snprintf(buffer + len, sizeof(buffer) - len,
                    "RtnCode=%d ElapsedTime=%.3f ", errorcode, elapsed);
  }

  /* Write the message to the log file. */
  _write_to_log(buffer, len);

  return 0;
}

int nslogit(const char *format, ...) {

  /* Variables */
  char    buffer[LOGBUFSZ * 2]; // 4096 * 2
  int     len;
  va_list args;

  /* Logging interfaced initialized? */
  if (app_init == 0)
    return 0;

  /* Retrieve the header part of the message. */
  len = _format_header(buffer, sizeof(buffer));

  /* Now for the message body, if the message is truncated reset the len
   * variable, there isn't much we can do about this.
   */
  va_start(args, format);
  len += vsnprintf(buffer + len, sizeof(buffer) - len, format, args);
  if (len >= (int)sizeof(buffer)) {
    len = sizeof(buffer);
  } else {
    len += 1;
  }
  va_end (args);

  /* Write the message to the log file. */
  _write_to_log(buffer, len);

  return 0;
}
