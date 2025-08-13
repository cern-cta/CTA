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
#include "json_logger.h"

#define HOST_NAME_MAX 24

static const char *short_hostname(void) {
  static char cached[HOST_NAME_MAX + 1];
  static int init = 0;

  if (!init) {
    if (gethostname(cached, sizeof cached) != 0) {
      strncpy(cached, "unknown", sizeof cached);
      cached[sizeof cached - 1] = '\0';
    } else {
      char *dot = strchr(cached, '.');
      if (dot) *dot = '\0';
    }
    init = 1;
  }
  return cached;
}

static void json_escape(const char *src, char *dst, size_t dst_size) {
    size_t out = 0;
    for (; *src && out + 1 < dst_size; src++) {
        unsigned char c = (unsigned char)*src;
        const char *esc = NULL;
        switch (c) {
            case '\"': esc = "\\\""; break;
            case '\\': esc = "\\\\"; break;
            case '\n': esc = "";  break;
            case '\r': esc = "";  break;
            case '\t': esc = "    ";  break;
            default:
                if (c < 0x20) {
                    /* Optional: hex escape for other control chars */
                    static char buf[7];
                    snprintf(buf, sizeof(buf), "\\u%04X", c);
                    esc = buf;
                }
                break;
        }
        if (esc) {
            size_t len = strlen(esc);
            if (out + len >= dst_size - 1) break;
            memcpy(dst + out, esc, len);
            out += len;
        } else {
            dst[out++] = (char)c;
        }
    }
    dst[out] = '\0';
}

int rmc_logit(const char *const lvl, const char *const msg) {
  // 1 for the msg buffer and 1 for the rest
  char prtbuf[2*RMC_PRTBUFSZ];
  int save_errno = save_errno = errno;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);

  struct tm local_tm;
  localtime_r(&ts.tv_sec, &local_tm);
  char local_time_str[64];
  strftime(local_time_str, sizeof(local_time_str), "%FT%T%z", &local_tm);

  const char *hostname = short_hostname();
  const char *program = "cta-rmcd";

  char esc_msg[RMC_PRTBUFSZ];
  json_escape(msg, esc_msg, sizeof esc_msg);
  snprintf(prtbuf, sizeof(prtbuf),
            "{\"epoch_time\":%lld.%09ld,"
            "\"local_time\":\"%s\","
            "\"hostname\":\"%s\","
            "\"program\":\"%s\","
            "\"log_level\":\"%s\","
            "\"pid\":%d,"
            "\"message\":\"%s\""
            "}\n",
            (long long)ts.tv_sec, ts.tv_nsec,
            local_time_str,
            hostname,
            program,
            lvl,
            getpid(),
            esc_msg);

  int fd_log = open("/var/log/cta/cta-rmcd.log", O_WRONLY | O_CREAT | O_APPEND, 0640);
  if(fd_log < 0) {
    errno = save_errno;
    return -1;
  }
  write(fd_log, prtbuf, strnlen(prtbuf, RMC_PRTBUFSZ));
  close(fd_log);
  errno = save_errno;
  return 0;
}

// There is some duplicate code in the following functions, but reducing this is
// not trivial due to the variadic arguments
// simpler to just keep it separate
int json_log_info(const char *const func, const char *const msg, ...) {
  va_list args;
  char msgbuf[RMC_PRTBUFSZ];
  int off = snprintf(msgbuf, sizeof msgbuf, "In %s(): ", func);
  if (off < 0) off = 0;
  if ((size_t)off >= sizeof msgbuf) off = (int)sizeof msgbuf - 1;

  va_start(args, msg);
  vsnprintf(msgbuf + off, sizeof msgbuf - (size_t)off, msg, args);
  va_end(args);

  return rmc_logit("INFO", msgbuf);
}

int json_log_err(const char *const func, const char *const msg, ...) {
  va_list args;
  char msgbuf[RMC_PRTBUFSZ];
  int off = snprintf(msgbuf, sizeof msgbuf, "In %s(): ", func);
  if (off < 0) off = 0;
  if ((size_t)off >= sizeof msgbuf) off = (int)sizeof msgbuf - 1;

  va_start(args, msg);
  vsnprintf(msgbuf + off, sizeof msgbuf - (size_t)off, msg, args);
  va_end(args);

  return rmc_logit("ERROR", msgbuf);
}

int json_log_warn(const char *const func, const char *const msg, ...) {
  va_list args;
  char msgbuf[RMC_PRTBUFSZ];
  int off = snprintf(msgbuf, sizeof msgbuf, "In %s(): ", func);
  if (off < 0) off = 0;
  if ((size_t)off >= sizeof msgbuf) off = (int)sizeof msgbuf - 1;

  va_start(args, msg);
  vsnprintf(msgbuf + off, sizeof msgbuf - (size_t)off, msg, args);
  va_end(args);

  return rmc_logit("WARN", msgbuf);
}
