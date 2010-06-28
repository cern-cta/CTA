/*
 * Copyright (C) 2000-2004 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_unlinkbyvid - remove all files on a volume */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_unlinkbyvid(char *server, const char *vid)
{
  /* Variables */
  char  func[16];
  char  sendbuf[REQBUFSZ];
  char  *q;
  char  *sbp;
  int   msglen;
  int   c;
  gid_t gid;
  uid_t uid;
  struct Cns_api_thread_info *thip;

  strcpy (func, "Cns_unlinkbyvid");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  /* Check that VID exists and is not too long */
  if (!vid) {
    serrno = EFAULT;
    return (-1);
  }
  if (strlen(vid) > CA_MAXVIDLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  /* Build the request header */
  sbp = sendbuf;
  marshall_LONG(sbp, CNS_MAGIC);
  marshall_LONG(sbp, CNS_UNLINKBYVID);
  q = sbp;                   /* Save the pointer, for field updates */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build the request body */
  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_STRING(sbp, vid);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen);  /* Update the length field */

  /* Send message to name server daemon */
  c = send2nsd(NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
