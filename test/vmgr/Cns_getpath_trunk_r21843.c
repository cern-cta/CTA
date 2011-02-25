/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_getpath - get full path corresponding to a fileid */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall_trunk_r21843.h"
#include "Cns_api_trunk_r21843.h"
#include "Cns_trunk_r21843.h"
#include "serrno_trunk_r21843.h"

int
Cns_getpath(char *server,
	    u_signed64 fileid,
	    char *path)
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "Cns_getpath", 16);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path) {
    serrno = EFAULT;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_GETPATH);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, fileid);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_STRING (rbp, path);
  }
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
