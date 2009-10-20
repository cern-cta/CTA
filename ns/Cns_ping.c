/*
 * Copyright (C) 2007 by CERN/IT/GD/ITR
 * All rights reserved
 */

/* Cns_ping - check name server alive and return version number */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_ping(char *host, char *info)
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *rbp;
  char repbuf[256];
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_ping");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    Cns_errmsg (func, NS053);
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  if (! info) {
    serrno = EFAULT;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_PING);
  q = sbp; /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, host, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_STRING (rbp, info);
  }
  return (c);
}
