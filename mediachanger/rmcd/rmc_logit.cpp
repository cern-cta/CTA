/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include "rmc_constants.hpp"
#include "rmc_logit.hpp"

/* Set in rmc_serv.c */
extern int g_jid;

int rmc_logit(const char* const func, const char* const msg, ...) {
  va_list args;
  char prtbuf[RMC_PRTBUFSZ];
  int save_errno;
  time_t current_time;
  int fd_log;

  save_errno = errno;
  va_start(args, msg);
  time(&current_time);

  struct tm localNowBuf;
  struct tm* tm = localtime_r(&current_time, &localNowBuf);
  snprintf(prtbuf,
           RMC_PRTBUFSZ,
           "%02d/%02d %02d:%02d:%02d %5d %s: ",
           tm->tm_mon + 1,
           tm->tm_mday,
           tm->tm_hour,
           tm->tm_min,
           tm->tm_sec,
           g_jid,
           func);
  size_t prtbufLen = strnlen(prtbuf, RMC_PRTBUFSZ);
  vsnprintf(prtbuf + prtbufLen, RMC_PRTBUFSZ - prtbufLen, msg, args);
  va_end(args);
  fd_log = open("/var/log/cta/cta-rmcd.log", O_WRONLY | O_CREAT | O_APPEND, 0640);
  if (fd_log < 0) {
    return -1;
  }
  write(fd_log, prtbuf, strnlen(prtbuf, RMC_PRTBUFSZ));
  close(fd_log);
  errno = save_errno;
  return 0;
}
