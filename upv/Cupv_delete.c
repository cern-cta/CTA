/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cupv_api.h"
#include "Cupv.h"
#include "serrno.h"
#include <string.h>

int Cupv_delete(uid_t priv_uid, gid_t priv_gid, const char *src, const char *tgt)
{
  int c;
  char func[16];
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cupv_api_thread_info *thip;
  uid_t uid;
  gid_t gid;
  int lensrc, lentgt;

  strncpy (func, "Cupv_delete", 16);
  if (Cupv_apiinit (&thip))
    return (-1);
  uid = geteuid();
  gid = getegid();

  if (src == NULL || tgt == NULL) {
    serrno = EINVAL;
    return(-1);
  }


  lensrc = strlen(src);
  lentgt = strlen(tgt);

  if (lensrc == 0 || lentgt == 0
      || lensrc > CA_MAXREGEXPLEN || lentgt > CA_MAXREGEXPLEN) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build request header */
  sbp = sendbuf;
  marshall_LONG (sbp, CUPV_MAGIC);
  marshall_LONG (sbp, CUPV_DELETE);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_LONG (sbp, priv_uid);
  marshall_LONG (sbp, priv_gid);
  marshall_STRING (sbp, src);
  marshall_STRING (sbp, tgt);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while ((c = send2Cupv (NULL, sendbuf, msglen, NULL, 0)) &&
	 serrno == ECUPVNACT)
    sleep (RETRYI);
  return (c);
}










