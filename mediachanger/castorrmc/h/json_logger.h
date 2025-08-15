/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
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

#pragma once

#include "osdep.h"
#include <stdbool.h>

enum LogLevel {
  L_DEBUG,
  L_INFO,
  L_WARN,
  L_ERR,
  L_CRIT
};

enum kv_type { KV_STRING, KV_STRING_UNQUOTED,KV_INT64, KV_DOUBLE, KV_BOOL };

struct kv {
  const char *key;
  enum kv_type type;
  union {
    const char *vstr;
    long long   vint64;
    double      vdouble;
    bool        vbool;
  } val;
};

#define KV_STR(k, v)      ((struct kv){ (k), KV_STRING, .val.vstr = (v) })
#define KV_STR_U(k, v)    ((struct kv){ (k), KV_STRING_UNQUOTED, .val.vstr = (v) })
#define KV_INT(k, v)      ((struct kv){ (k), KV_INT64,  .val.vint64 = (v) })
#define KV_DOUBLE(k, v)   ((struct kv){ (k), KV_DOUBLE, .val.vdouble = (v) })
#define KV_BOOL(k, v)     ((struct kv){ (k), KV_BOOL,   .val.vbool = (v) })


#define JSON_LOG(lvl, msg, ...) \
  json_log_kv((lvl), __FUNCTION__, NULL, 0, (msg), ##__VA_ARGS__)
#define JSON_LOG_WITH_FIELDS(lvl, fields, msg, ...) \
  json_log_kv((lvl), __FUNCTION__,  (fields), sizeof(fields)/sizeof((fields)[0]), (msg), ##__VA_ARGS__)

EXTERN_C int json_log_kv(enum LogLevel lvl, const char *const func, const struct kv *fields, size_t nfields, const char *const msg, ...);
