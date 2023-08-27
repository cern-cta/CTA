/*
 * @project      The CERN Tape Archive(CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3(GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or(at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include "rmc_constants.h"
#include "rmc_logit.h"

/* Set in rmc_serv.c */
extern int g_jid;

int rmc_logit(const char *const func, const char *const msg, ...) {
  va_list args;
  char prtbuf[RMC_PRTBUFSZ];
  int save_errno;
  struct tm *tm;
  time_t current_time;
  int fd_log;

  save_errno = errno;
  va_start(args, msg);
  time(&current_time);
  tm = localtime(&current_time);
  snprintf(prtbuf, RMC_PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, g_jid, func);
  size_t prtbufLen = strnlen(prtbuf, RMC_PRTBUFSZ);
  vsnprintf(prtbuf+prtbufLen, RMC_PRTBUFSZ-prtbufLen, msg, args);
  va_end(args);
  fd_log = open("/var/log/cta/cta-rmcd.log", O_WRONLY | O_CREAT | O_APPEND, 0664);
  if(fd_log < 0) return -1;
  write(fd_log, prtbuf, strnlen(prtbuf, RMC_PRTBUFSZ));
  close(fd_log);
  errno = save_errno;
  return 0;
}
