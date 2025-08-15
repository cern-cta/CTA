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
#include <stdlib.h>
#include "rmc_constants.h"
#include "json_logger.h"

#define HOST_NAME_MAX 24

static const char* short_hostname(void) {
  static char cached[HOST_NAME_MAX + 1];
  static int init = 0;

  if (!init) {
    if (gethostname(cached, sizeof cached) != 0) {
      strncpy(cached, "unknown", sizeof cached);
      cached[sizeof cached - 1] = '\0';
    } else {
      char* dot = strchr(cached, '.');
      if (dot) {
        *dot = '\0';
      }
    }
    init = 1;
  }
  return cached;
}

static const char* getLogLevel(enum LogLevel lvl) {
  static const char* logLevels[] = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"};
  size_t numLogLevels = sizeof(logLevels) / sizeof(logLevels[0]);
  if (lvl < 0 || lvl >= numLogLevels) {
    // Just log with info in this case
    return "INFO";
  }
  return logLevels[lvl];
}

static void json_escape(const char* src, char* dst, size_t dst_size) {
  size_t out = 0;
  for (; *src && out + 1 < dst_size; src++) {
    unsigned char c = (unsigned char) *src;
    const char* esc = NULL;
    switch (c) {
      case '\"':
        esc = "\\\"";
        break;
      case '\\':
        esc = "\\\\";
        break;
      case '\n':
        esc = "";
        break;
      case '\r':
        esc = "";
        break;
      case '\t':
        esc = "    ";
        break;
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
      if (out + len >= dst_size - 1) {
        break;
      }
      memcpy(dst + out, esc, len);
      out += len;
    } else {
      dst[out++] = (char) c;
    }
  }
  dst[out] = '\0';
}

int log_to_file(const char* const msg) {
  int fd_log = open("/var/log/cta/cta-rmcd.log", O_WRONLY | O_CREAT | O_APPEND, 0640);
  if (fd_log < 0) {
    return -1;
  }

  write(fd_log, msg, strnlen(msg, RMC_PRTBUFSZ));
  close(fd_log);
  return 0;
}

void write_kv(FILE* logstream, const struct kv field) {
  switch (field.type) {
    case KVT_STR: {
      char esc_val[RMC_PRTBUFSZ];
      json_escape(field.val.vstr ? field.val.vstr : "", esc_val, sizeof esc_val);
      fprintf(logstream, "\"%s\":\"%s\"", field.key, esc_val);
    } break;
    case KVT_STR_UNQUOTED: {
      char esc_val[RMC_PRTBUFSZ];
      json_escape(field.val.vstr ? field.val.vstr : "", esc_val, sizeof esc_val);
      fprintf(logstream, "\"%s\":%s", field.key, esc_val);
    } break;
    case KVT_INT64:
      fprintf(logstream, "\"%s\":%lld", field.key, (long long) field.val.vint64);
      break;
    case KVT_DOUBLE:
      fprintf(logstream, "\"%s\":%.17g", field.key, field.val.vdouble);
      break;
    case KVT_BOOL:
      fprintf(logstream, "\"%s\":%s", field.key, field.val.vbool ? "true" : "false");
      break;
  }
}

char* constr_log_line(enum LogLevel lvl, const char* msg, const struct kv* fields, size_t nfields) {
  const int short_buffer_size = 64;

  // Get some timing info
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm local_tm;
  localtime_r(&ts.tv_sec, &local_tm);
  char local_time_str[short_buffer_size];
  strftime(local_time_str, sizeof(local_time_str), "%FT%T%z", &local_tm);
  char epochbuf[short_buffer_size];
  snprintf(epochbuf, short_buffer_size, "%lld.%09ld", (long long) ts.tv_sec, ts.tv_nsec);

  struct kv base_fields[] = {KV_STR_U("epoch_time", epochbuf),
                             KV_STR("local_time", local_time_str),
                             KV_STR("hostname", short_hostname()),
                             KV_STR("program", "cta-rmcd"),
                             KV_STR("log_level", getLogLevel(lvl)),
                             KV_INT("pid", getpid()),
                             KV_STR("message", msg)};
  const size_t nbase_fields = 7;

  // Build string
  char* buf = NULL;
  size_t len = 0;
  FILE* logstream = open_memstream(&buf, &len);
  if (!logstream) {
    return NULL;
  }
  fprintf(logstream, "{");
  for (size_t i = 0; i < nbase_fields; i++) {
    write_kv(logstream, base_fields[i]);
    if (!(i == nbase_fields - 1 && nfields == 0)) {
      fprintf(logstream, ",");
    }
  }
  for (size_t i = 0; i < nfields; i++) {
    write_kv(logstream, fields[i]);
    if (i != nfields - 1) {
      fprintf(logstream, ",");
    }
  }
  fprintf(logstream, "}\n");
  fflush(logstream);
  fclose(logstream);

  return buf;
}

int json_log_kv(enum LogLevel lvl,
                const char* const func,
                const struct kv* fields,
                size_t nfields,
                const char* const msg,
                ...) {
  int save_errno = errno;

  va_list args;
  char msgbuf[RMC_PRTBUFSZ];
  int off = snprintf(msgbuf, sizeof msgbuf, "In %s(): ", func);
  if (off < 0) {
    off = 0;
  }
  if ((size_t) off >= sizeof msgbuf) {
    off = (int) sizeof msgbuf - 1;
  }

  va_start(args, msg);
  vsnprintf(msgbuf + off, sizeof msgbuf - (size_t) off, msg, args);
  va_end(args);

  char* log_line = constr_log_line(lvl, msgbuf, fields, nfields);
  if (log_line == NULL) {
    return -1;
  }
  int success = log_to_file(log_line);
  free(log_line);
  errno = save_errno;
  return success;
}
