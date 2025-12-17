/*
 * SPDX-FileCopyrightText: 2002 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/*      rmc_mount - mount a cartridge on a drive */

#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/serrno.hpp"
#include "rmc_api.hpp"

#include <netinet/in.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

int rmc_mount(const char* const server, const char* const vid, const int side, const int drvord) {
  int c;
  gid_t gid;
  int msglen;
  char* q;
  char repbuf[1];
  char* sbp;
  char sendbuf[RMC_REQBUFSZ];
  uid_t uid;

  uid = getuid();
  gid = getgid();

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG(sbp, RMC_MAGIC);
  marshall_LONG(sbp, RMC_MOUNT);
  q = sbp; /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build request body */

  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_STRING(sbp, ""); /* loader field is no longer used */
  marshall_STRING(sbp, vid);
  marshall_SHORT(sbp, side);
  marshall_SHORT(sbp, drvord);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen); /* update length field */

  while ((c = send2rmc(server, sendbuf, msglen, repbuf, sizeof(repbuf))) && serrno == ERMCNACT) {
    sleep(RMC_RETRYI);
  }
  return c;
}
