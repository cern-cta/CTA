/*
 * $Id: stager_util.c,v 1.4 2009/08/18 09:43:01 waldron Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>
#include "osdep.h"
#include "stager_util.h"
#include "Cns_api.h"
#include "serrno.h"
#include "u64subr.h"
#include "stager_extern_globals.h"
#include "stager_messages.h"
#include "stager_uuid.h"
#include "log.h"

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
