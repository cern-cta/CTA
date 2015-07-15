/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_dropsegs - drop all segments of a file */

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
Cns_dropsegs(const char *path, struct Cns_fileid *file_uniqueid) {
  char *actual_path;
  char func[16];
  struct Cns_api_thread_info *thip;
  uid_t uid;
  gid_t gid;
  int c;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  u_signed64 zero = 0;

  strncpy (func, "Cns_dropsegs", 16);
  if (Cns_apiinit (&thip)) {
    return (-1);
  }
  Cns_getid(&uid, &gid);
  if ((! path && ! file_uniqueid)) {
    serrno = EFAULT;
    return (-1);
  }

  if (path && strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  if (file_uniqueid && *file_uniqueid->server)
    strcpy (server, file_uniqueid->server);
  else
    if (Cns_selectsrvr (path, thip->server, server, &actual_path))
      return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC6);
  marshall_LONG (sbp, CNS_DROPSEGS);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  if (file_uniqueid && *file_uniqueid->server) {
    marshall_HYPER (sbp, file_uniqueid->fileid);
    marshall_STRING (sbp, "");
  } else {
    marshall_HYPER (sbp, zero);
    marshall_STRING (sbp, actual_path);
  }
  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
