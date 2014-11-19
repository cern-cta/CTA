/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cupv_api.h"
#include "Cupv.h"
#include "serrno.h"
#include "getconfent.h"
#include <string.h>


int Cupv_check(uid_t priv_uid, gid_t priv_gid, const char *src, const char *tgt, int priv)
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cupv_api_thread_info *thip;
  uid_t uid;
  int lensrc, lentgt;

  strncpy (func, "Cupv_check", 16);
  if (Cupv_apiinit (&thip))
    return (-1);
  uid = geteuid();
  gid = getegid();

  if (priv < 0) {
    serrno = EINVAL;
    return (-1);
  }

  if (src == NULL) {
    lensrc = 0;
  } else {
    lensrc = strlen(src);
  }

  if (tgt == NULL) {
    lentgt = 0;
  } else {
    lentgt = strlen(tgt);
  }

  if (lensrc > CA_MAXREGEXPLEN || lentgt > CA_MAXREGEXPLEN) {
    serrno = EINVAL;
    return(-1);
  }

  /* Applying a first check to see if the request is for root or for the user running this daemon */
  /* In this case just return without asking the server */
  if (priv_uid == 0 || priv_uid == getuid()) {
    if (src == NULL && tgt == NULL) {
      /* Both NULL, authorized */
      return(0);
    } else if ((src != NULL) && (tgt != NULL) && (strcmp(src, tgt)==0)) {
      /* src == tgt */
      return(0);
    }
  } /* In other cases, a message is sent to the server for validation */

  /* Build request header */
  sbp = sendbuf;
  marshall_LONG (sbp, CUPV_MAGIC);
  marshall_LONG (sbp, CUPV_CHECK);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_LONG (sbp, priv_uid);
  marshall_LONG (sbp, priv_gid);

  if (src == NULL) {
    marshall_STRING (sbp, "");
  } else {
    marshall_STRING(sbp, src);
  }

  if (tgt == NULL) {
    marshall_STRING (sbp, "");
  } else {
    marshall_STRING(sbp, tgt);
  }

  marshall_LONG (sbp, priv);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while ((c = send2Cupv (NULL, sendbuf, msglen, NULL, 0)) &&
	 serrno == ECUPVNACT)
    sleep (RETRYI);
  return (c);
}
