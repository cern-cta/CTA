/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rmc_api.hpp"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

static char* errbufp = nullptr;
static int errbuflen;

void rmc_seterrbuf(char* buffer, int buflen) {
  errbufp = buffer;
  errbuflen = buflen;
}

int rmc_errmsg(const char* const func, const char* const msg, ...) {
  va_list args;
  char prtbuf[RMC_PRTBUFSZ];
  size_t prtbufLen;
  int save_errno;

  save_errno = errno;
  va_start(args, msg);
  if (func) {
    snprintf(prtbuf, RMC_PRTBUFSZ, "%s: ", func);
    prtbufLen = strlen(prtbuf);
  } else {
    *prtbuf = '\0';
    prtbufLen = 0;
  }
  vsnprintf(prtbuf + prtbufLen, RMC_PRTBUFSZ - prtbufLen, msg, args);
  if (errbufp) {
    errbufp[errbuflen - 1] = '\0';
    strncpy(errbufp, prtbuf, errbuflen);
    if (errbufp[errbuflen - 1] != '\0') {
      errbufp[errbuflen - 2] = '\n';
      errbufp[errbuflen - 1] = '\0';
    }
  } else {
    fprintf(stderr, "%s", prtbuf);
  }
  va_end(args);
  errno = save_errno;
  return 0;
}
