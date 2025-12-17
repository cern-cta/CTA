/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "rmc_sendrep.hpp"

#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/net.hpp"
#include "rmc_constants.hpp"
#include "rmc_logit.hpp"

#include <errno.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

static const char* rep_type_to_str(const int rep_type) {
  switch (rep_type) {
    case MSG_ERR:
      return "MSG_ERR";
    case MSG_DATA:
      return "MSG_DATA";
    case RMC_RC:
      return "RMC_RC";
    default:
      return "UNKNOWN";
  }
}

int rmc_sendrep(const int rpfd, const int rep_type, ...) {
  va_list args;
  char* msg;
  int n;
  char prtbuf[RMC_PRTBUFSZ];
  char* rbp;
  int rc;
  char repbuf[RMC_REPBUFSZ];
  int repsize;
  const char* const func = "rmc_sendrep";

  rbp = repbuf;
  marshall_LONG(rbp, RMC_MAGIC);
  va_start(args, rep_type);
  marshall_LONG(rbp, rep_type);
  switch (rep_type) {
    case MSG_ERR:
      msg = va_arg(args, char*);
      vsprintf(prtbuf, msg, args);
      marshall_LONG(rbp, strlen(prtbuf) + 1);
      marshall_STRING(rbp, prtbuf);
      rmc_logit(func, "%s", prtbuf);
      break;
    case MSG_DATA:
      n = va_arg(args, int);
      marshall_LONG(rbp, n);
      msg = va_arg(args, char*);
      memcpy(rbp, msg, n); /* marshalling already done */
      rbp += n;
      break;
    case RMC_RC:
      rc = va_arg(args, int);
      marshall_LONG(rbp, rc);
      break;
  }
  va_end(args);
  repsize = rbp - repbuf;
  if (netwrite(rpfd, repbuf, repsize) != repsize) {
    const char* const neterror_str = neterror();
    rmc_logit(func, RMC02, "send", neterror_str);
    rmc_logit(func,
              "Call to netwrite() failed"
              ": rep_type=%s neterror=%s\n",
              rep_type_to_str(rep_type),
              neterror_str);
    if (rep_type == RMC_RC) {
      close(rpfd);
    }
    return -1;
  }
  if (rep_type == RMC_RC) {
    close(rpfd);
  }
  return 0;
}
