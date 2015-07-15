/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* Cns_setacl - set the Access Control List for a file/directory */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int
Cns_setacl(const char *path, int nentries, struct Cns_acl *acl)
{
  char *actual_path;
  int c;
  char func[16];
  gid_t gid;
  int i;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "Cns_setacl", 16);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getrealid(&uid, &gid);

  if (! path || ! acl) {
    serrno = EFAULT;
    return (-1);
  }

  if (strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }
  if (nentries <= 0 || nentries > CA_MAXACLENTRIES) {
    serrno = EINVAL;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_SETACL);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);
  marshall_WORD (sbp, nentries);
  for (i = 0; i < nentries; i++) {
    marshall_BYTE (sbp, acl->a_type);
    marshall_LONG (sbp, acl->a_id);
    marshall_BYTE (sbp, acl->a_perm);
    acl++;
  }

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
