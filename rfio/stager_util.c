/*
 * $Id: stager_util.c,v 1.2 2008/07/31 07:09:14 sponcec3 Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <winsock2.h>
#endif
#include <stdarg.h>
#include "osdep.h"
#include "stager_util.h"
#include "Cns_api.h"
#include "serrno.h"
#include "u64subr.h"
#include "stager_extern_globals.h"
#include "stager_messages.h"
#include "stager_uuid.h"
#include "dlf_api.h"
#include "log.h"

#ifdef SIXMONTHS
#undef SIXMONTHS
#endif
#define SIXMONTHS (6*30*24*60*60)

#if defined(_WIN32)
static char strftime_format_sixmonthsold[] = "%b %d %Y";
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

void DLL_DECL stager_util_time(this,timestr)
     time_t this;
     char *timestr;
{
  time_t this_time = time(NULL);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
  struct tm *tp;

#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
  localtime_r(&(this),&tmstruc);
  tp = &tmstruc;
#else
  tp = localtime(&(this));
#endif /* _REENTRANT || _THREAD_SAFE */
  if ((this_time >= this) && ((this_time - this) > SIXMONTHS)) {
    /* Too much in past */
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else if ((this_time < this) && ((this - this_time) > SIXMONTHS)) {
    /* Too much in feature...! */
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
    strftime(timestr,64,strftime_format,tp);
  }
}

void DLL_DECL stager_log(const char *func, const char *file, int line, int what, struct Cns_fileid *fileid, ...)
{
  va_list args;
  char *message;
  char *valueStr;
  int   valueInt;
  void *valueVoid;
  char *message2;
  char *value2Str;
  int value2Int;
  /* Gcc does not like statement like: xxxx ? 1 : "" */
  /* It will issue a warning about type mismatch... */
  /* So I will convert the integers to string */
  int _save_serrno = serrno;

  va_start(args, fileid);

  message = va_arg(args, char *);
  if (message != NULL) {
    if ((strcmp(message, "STRING") == 0) || (strcmp(message, "SIGNAL NAME") == 0)) {
      valueStr = va_arg(args, char *);
    } else {
      valueInt = va_arg(args, int);
    }
    message2 = va_arg(args, char *);
    if (message2 != NULL) {
      if ((strcmp(message2, "STRING") == 0) || (strcmp(message2, "SIGNAL NAME") == 0)) {
        value2Str = va_arg(args, char *);
      } else {
        value2Int = va_arg(args, int);
      }

      if (! stagerNoDlf) {
        if ((strcmp(message, "STRING") == 0) || (strcmp(message, "SIGNAL NAME") == 0)) {
          if ((strcmp(message2, "STRING") == 0) || (strcmp(message2, "SIGNAL NAME") == 0)) {
            dlf_write(
                      stager_request_uuid,
                      stagerMessages[what].defaultSeverity,
                      what,
                      fileid,
                      STAGER_NB_PARAMS+3,
                      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                      message,DLF_MSG_PARAM_STR,valueStr,
                      message2,DLF_MSG_PARAM_STR,value2Str,
                      STAGER_LOG_WHERE(file,line)
                      );
          } else {
            dlf_write(
                      stager_request_uuid,
                      stagerMessages[what].defaultSeverity,
                      what,
                      fileid,
                      STAGER_NB_PARAMS+3,
                      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                      message,DLF_MSG_PARAM_STR,valueStr,
                      message2,DLF_MSG_PARAM_INT,value2Int,
                      STAGER_LOG_WHERE(file,line)
                      );
          }
        } else {
          if ((strcmp(message2, "STRING") == 0) || (strcmp(message2, "SIGNAL NAME") == 0)) {
            dlf_write(
                      stager_request_uuid,
                      stagerMessages[what].defaultSeverity,
                      what,
                      fileid,
                      STAGER_NB_PARAMS+3,
                      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                      message,DLF_MSG_PARAM_INT,valueInt,
                      message2,DLF_MSG_PARAM_STR,value2Str,
                      STAGER_LOG_WHERE(file,line)
                      );
          } else {
            dlf_write(
                      stager_request_uuid,
                      stagerMessages[what].defaultSeverity,
                      what,
                      fileid,
                      STAGER_NB_PARAMS+3,
                      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                      message,DLF_MSG_PARAM_INT,valueInt,
                      message2,DLF_MSG_PARAM_INT,value2Int,
                      STAGER_LOG_WHERE(file,line)
                      );
          }
        }

      }
      if ((stagerLog != NULL) && (stagerLog[0] != '\0')) {
        if ((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) {
          if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) {
            log(
                stagerMessages[what].severity2LogLevel,
                "%s : %s:%d : %s : %s : %s (errno=%d [%s], serrno=%d[%s])\n",
                func,
                file,
                line,
                stagerMessages[what].messageTxt,
                valueStr,
                value2Str,
                errno,
                errno ? strerror(errno) : "",
                serrno,
                serrno ? sstrerror(serrno) : ""
                );
          } else {
            log(
                stagerMessages[what].severity2LogLevel,
                "%s : %s:%d : %s : %s : %d (errno=%d [%s], serrno=%d[%s])\n",
                func,
                file,
                line,
                stagerMessages[what].messageTxt,
                valueStr,
                value2Int,
                errno, errno ? strerror(errno) : "",
                serrno, serrno ? sstrerror(serrno) : ""
                );
          }
        } else {
          if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) {
            log(
                stagerMessages[what].severity2LogLevel,
                "%s : %s:%d : %s : %d : %s (errno=%d [%s], serrno=%d[%s])\n",
                func,
                file,
                line,
                stagerMessages[what].messageTxt,
                valueInt,
                value2Str,
                errno, errno ? strerror(errno) : "",
                serrno, serrno ? sstrerror(serrno) : ""
                );
          } else {
            log(
                stagerMessages[what].severity2LogLevel,
                "%s : %s:%d : %s : %d : %d (errno=%d [%s], serrno=%d[%s])\n",
                func,
                file,
                line,
                stagerMessages[what].messageTxt,
                valueInt,
                value2Int,
                errno,
                errno ? strerror(errno) : "",
                serrno, serrno ? sstrerror(serrno) : ""
                );
          }
        }
      }
    } else {
      if (! stagerNoDlf) {
        if ((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) {
          dlf_write(
                    stager_request_uuid,
                    stagerMessages[what].defaultSeverity,
                    what,
                    fileid,
                    STAGER_NB_PARAMS+2,
                    stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                    message,DLF_MSG_PARAM_STR,valueStr,
                    STAGER_LOG_WHERE(file,line)
                    );
        } else {
          dlf_write(
                    stager_request_uuid,
                    stagerMessages[what].defaultSeverity,
                    what,
                    fileid,
                    STAGER_NB_PARAMS+2,
                    stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                    message,DLF_MSG_PARAM_INT,valueInt,
                    STAGER_LOG_WHERE(file,line)
                    );
        }
      }

      if ((stagerLog != NULL) && (stagerLog[0] != '\0')) {
        if ((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) {
          log(
              stagerMessages[what].severity2LogLevel,
              "%s : %s:%d : %s : %s (errno=%d [%s], serrno=%d[%s])\n",
              func,
              file,
              line,
              stagerMessages[what].messageTxt,
              valueStr,
              errno, errno ? strerror(errno) : "",
              serrno, serrno ? sstrerror(serrno) : ""
              );
        } else {
          log(
              stagerMessages[what].severity2LogLevel,
              "%s : %s:%d : %s : %d (errno=%d [%s], serrno=%d[%s])\n",
              func,
              file,
              line,
              stagerMessages[what].messageTxt,
              valueInt,
              errno,
              errno ? strerror(errno) : "",
              serrno,
              serrno ? sstrerror(serrno) : ""
              );
        }
      }
    }
  } else {
    valueVoid = va_arg(args, void *);
    message2 = va_arg(args, char *);
    if (message2 != NULL) {
      if ((strcmp(message2, "STRING") == 0) || (strcmp(message2, "SIGNAL NAME") == 0)) {
        value2Str = va_arg(args, char *);
      } else {
        value2Int = va_arg(args, int);
      }
      if (! stagerNoDlf) {
        if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) {
          dlf_write(
                    stager_request_uuid,
                    stagerMessages[what].defaultSeverity,
                    what,
                    fileid,
                    STAGER_NB_PARAMS+2,
                    stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                    message2,DLF_MSG_PARAM_STR,value2Str,
                    STAGER_LOG_WHERE(file,line)
                    );
        } else {
          dlf_write(
                    stager_request_uuid,
                    stagerMessages[what].defaultSeverity,
                    what,
                    fileid,
                    STAGER_NB_PARAMS+2,
                    stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func,
                    message2,DLF_MSG_PARAM_INT,value2Int,
                    STAGER_LOG_WHERE(file,line)
                    );
        }
      }
      if ((stagerLog != NULL) && (stagerLog[0] != '\0')) {
        if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) {
          log(
              stagerMessages[what].severity2LogLevel,
              "%s : %s:%d : %s : %s\n",
              func,
              file,
              line,
              stagerMessages[what].messageTxt,
              value2Str
              );
        } else {
          log(
              stagerMessages[what].severity2LogLevel,
              "%s : %s:%d : %s : %d\n",
              func,
              file,
              line,
              stagerMessages[what].messageTxt,
              value2Int
              );
        }
      }
    } else {
      if (! stagerNoDlf)
        dlf_write(
                  stager_request_uuid,
                  stagerMessages[what].defaultSeverity,
                  what,
                  (struct Cns_fileid *)fileid,
                  STAGER_NB_PARAMS+1,
                  stagerMessages[what].what2Type,
                  DLF_MSG_PARAM_STR,
                  func,
                  STAGER_LOG_WHERE(file,line)
                  );
      if ((stagerLog != NULL) && (stagerLog != '\0')) {
        log(
            stagerMessages[what].severity2LogLevel,
            "%s : %s:%d : %s\n",
            func,
            file,
            line,
            stagerMessages[what].messageTxt
            );
      }
    }
  }
  serrno = _save_serrno;
  va_end(args);
}
