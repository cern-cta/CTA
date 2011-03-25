/*
 * Copyright (C) 2000-2004 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_closex - close a file */

/* ---- FOR INTERNAL USE ONLY! ---- */

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
Cns_closex(const struct Cns_fileid *file_uniqueid,
           const u_signed64 filesize,
           const char *csumtype,
           const char *csumvalue,
           const time_t new_mod_time,
           const time_t last_mod_time,
           struct Cns_filestatcs *statbuf)
{
  /* Variables */
  struct Cns_api_thread_info *thip;
  char  *q;
  char  *rbp;
  char  *sbp;
  char  func[11];
  char  repbuf[93];
  char  sendbuf[REQBUFSZ];
  char  server[CA_MAXHOSTNAMELEN+1];
  gid_t gid;
  int   c;
  int   msglen;
  uid_t uid;

  strncpy (func, "Cns_closex", 11);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! file_uniqueid) {
    serrno = EFAULT;
    return (-1);
  }

  if ((csumtype && strlen (csumtype) > 2) ||
      (csumvalue && strlen (csumvalue) > CA_MAXCKSUMLEN) ||
      (file_uniqueid->server[0] == '\0')) {
    serrno = EINVAL;
    return (-1);
  }
  strcpy (server, file_uniqueid->server);

  /* Build request header */
  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_CLOSEX);
  q = sbp;  /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */
  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, file_uniqueid->fileid);
  marshall_HYPER (sbp, filesize);
  marshall_STRING (sbp, csumtype);
  marshall_STRING (sbp, csumvalue);
  marshall_TIME_T (sbp, new_mod_time);
  marshall_TIME_T (sbp, last_mod_time);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);  /* Update length field */

  /* Send request */
  c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {

    /* Unmarshall response */
    if (statbuf != NULL) {
      rbp = repbuf;
      unmarshall_HYPER (rbp, statbuf->fileid);
      unmarshall_WORD (rbp, statbuf->filemode);
      unmarshall_LONG (rbp, statbuf->nlink);
      unmarshall_LONG (rbp, statbuf->uid);
      unmarshall_LONG (rbp, statbuf->gid);
      unmarshall_HYPER (rbp, statbuf->filesize);
      unmarshall_TIME_T (rbp, statbuf->atime);
      unmarshall_TIME_T (rbp, statbuf->mtime);
      unmarshall_TIME_T (rbp, statbuf->ctime);
      unmarshall_WORD (rbp, statbuf->fileclass);
      unmarshall_BYTE (rbp, statbuf->status);
      unmarshall_STRING (rbp, statbuf->csumtype);
      unmarshall_STRING (rbp, statbuf->csumvalue);
    }
  }
  return (c);
}
