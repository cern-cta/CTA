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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include "rmc_constants.h"

static char *errbufp = NULL;
static int errbuflen;

void rmc_seterrbuf(char *buffer, int buflen)
{
  errbufp = buffer;
  errbuflen = buflen;
}

int rmc_errmsg(char *func, char *msg, ...)
{
  va_list args;
  char prtbuf[RMC_PRTBUFSZ];
  size_t prtbufLen;
  int save_errno;

  save_errno = errno;
  va_start(args, msg);
  if(func) {
    snprintf(prtbuf, RMC_PRTBUFSZ, "%s: ", func);
    prtbufLen = strlen(prtbuf);
  } else {
    *prtbuf = '\0';
    prtbufLen = 0;
  }
  vsnprintf(prtbuf+prtbufLen, RMC_PRTBUFSZ-prtbufLen, msg, args);
  if(errbufp) {
    errbufp[errbuflen-1] = '\0';
    strncpy(errbufp, prtbuf, errbuflen);
    if(errbufp[errbuflen-1] != '\0') {
      errbufp[errbuflen-2] = '\n';
      errbufp[errbuflen-1] = '\0';
    }
  } else {
    fprintf(stderr, "%s", prtbuf);
  }
  va_end(args);
  errno = save_errno;
  return 0;
}
