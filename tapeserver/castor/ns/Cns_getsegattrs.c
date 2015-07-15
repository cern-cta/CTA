/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_getsegattrs - get file segments attributes */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"
#include <string.h>

int
Cns_getsegattrs(const char *path, struct Cns_fileid *file_uniqueid, int *nbseg, struct Cns_segattrs **segattrs)
{
  char *actual_path;
  int c;
  char func[16];
  gid_t gid;
  int i;
  int msglen;
  char *q;
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  struct Cns_segattrs *segments = NULL;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;
  u_signed64 zero = 0;

  strncpy (func, "Cns_getsegattrs", 16);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if ((! path && ! file_uniqueid) || ! nbseg || ! segattrs) {
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
  marshall_LONG (sbp, CNS_MAGIC4);
  marshall_LONG (sbp, CNS_GETSEGAT);
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

  c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_WORD (rbp, *nbseg);
    if (*nbseg == 0) {
      *segattrs = NULL;
      return (0);
    }
    segments = malloc (*nbseg * sizeof(struct Cns_segattrs));
    if (segments == NULL) {
      serrno = ENOMEM;
      return (-1);
    }
    for (i = 0; i < *nbseg; i++) {
      unmarshall_WORD (rbp, segments[i].copyno);
      unmarshall_WORD (rbp, segments[i].fsec);
      unmarshall_HYPER (rbp, segments[i].segsize);
      unmarshall_LONG (rbp, segments[i].compression);
      unmarshall_BYTE (rbp, segments[i].s_status);
      unmarshall_STRING (rbp, segments[i].vid);
      unmarshall_WORD (rbp, segments[i].side);
      unmarshall_LONG (rbp, segments[i].fseq);
      unmarshall_OPAQUE (rbp, segments[i].blockid, 4);
      unmarshall_STRINGN (rbp, segments[i].checksum_name,
                          CA_MAXCKSUMNAMELEN);
      unmarshall_LONG (rbp, segments[i].checksum);
    }
    *segattrs = segments;
  }
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
