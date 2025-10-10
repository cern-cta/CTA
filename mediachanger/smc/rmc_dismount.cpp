/*
 * SPDX-FileCopyrightText: 2002 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/*      rmc_dismount - dismount a cartridge from a drive */

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/serrno.hpp"
#include "rmc_api.hpp"

int rmc_dismount(const char* const server, const char* const vid, const int drvord, const int force) {
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
  marshall_LONG(sbp, RMC_UNMOUNT);
  q = sbp; /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build request body */

  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_STRING(sbp, ""); /* loader field is no longer used */
  marshall_STRING(sbp, vid);
  marshall_WORD(sbp, drvord);
  marshall_WORD(sbp, force);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen); /* update length field */

  while ((c = send2rmc(server, sendbuf, msglen, repbuf, sizeof(repbuf))) && serrno == ERMCNACT) {
    sleep(RMC_RETRYI);
  }
  return (c);
}
