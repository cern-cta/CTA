/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

/* Cns_getgrpbynam - get gid associated with a given group name */

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
Cns_getgrpbynam(char *groupname, gid_t *gid)
{
  int c;
  char func[16];
  int msglen;
  int n;
  char *q;
  char *rbp;
  char repbuf[4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;

  strcpy (func, "Cns_getgrpbynam");
  if (Cns_apiinit (&thip))
    return (-1);

  if (! groupname || ! gid) {
    serrno = EFAULT;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_GETGRPID);
  q = sbp; /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_STRING (sbp, groupname);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_LONG (rbp, n);
    *gid = n;
  }
  return (c);
}
