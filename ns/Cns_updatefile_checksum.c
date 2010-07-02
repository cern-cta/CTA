/*
 * Copyright (C) 2002 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_updatefile_checksum - updates the file checksum */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"
#include <string.h>

int
Cns_updatefile_checksum(const char *path, const char *csumtype, const char *csumvalue)
{
  char *actual_path;
  int c;
  char func[24];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_updatefile_checksum");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path ) {
    serrno = EFAULT;
    return (-1);
  }

  if (path && strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  if ((csumtype && strlen (csumtype) > 2) || 
      (csumvalue && strlen (csumvalue) > CA_MAXCKSUMLEN)) {
    serrno = EINVAL;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_UPDATEFILE_CHECKSUM);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);

  if (csumtype) {
    marshall_STRING (sbp, csumtype);
  } else {
    marshall_STRING (sbp, "");
  }
  if (csumvalue) {
    marshall_STRING (sbp, csumvalue);
  } else {
    marshall_STRING (sbp, "");
  }

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
