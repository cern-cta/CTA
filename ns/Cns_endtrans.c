/*
 * Copyright (C) 2004 by CERN/IT/GD/CT
 * All rights reserved
 */

/* Cns_endtrans - end transaction mode */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_endtrans()
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_endtrans");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_ENDTRANS);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (&thip->fd, NULL, sendbuf, msglen, NULL, 0);
  if (c == 0)
    thip->fd = -1;
  return (c);
}
