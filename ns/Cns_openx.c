/*
 * Copyright (C) 2000-2004 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_openx - open a file */

/* ---- FOR INTERNAL USE ONLY! ---- */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "rfcntl.h"
#include "serrno.h"

int
Cns_openx(const uid_t owneruid,
          const gid_t ownergid,
          const char *path,
          const int flags,
          const mode_t mode,
          const int classid,
          int *newfile,
          struct Cns_fileid *file_uniqueid,
          struct Cns_filestatcs *statbuf)
{
  /* Variables */
  struct Cns_api_thread_info *thip;
  char  *actual_path;
  char  *q;
  char  *rbp;
  char  *sbp;
  char  func[10];
  char  repbuf[REPBUFSZ];
  char  sendbuf[REQBUFSZ];
  char  server[CA_MAXHOSTNAMELEN + 1];
  gid_t gid;
  int   c;
  int   msglen;
  uid_t uid;

  strncpy (func, "Cns_openx", 10);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path || ! file_uniqueid) {
    serrno = EFAULT;
    return (-1);
  }

  /* Initialize the Cns_fileid structure */
  file_uniqueid->fileid    = 0;
  file_uniqueid->server[0] = 0;

  if (strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  /* Determine the name of the remote name server host */
  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */
  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_OPENX);
  q = sbp;  /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */
  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_LONG (sbp, owneruid);
  marshall_LONG (sbp, ownergid);
  marshall_WORD (sbp, thip->mask);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);
  marshall_LONG (sbp, htonopnflg(flags));
  marshall_LONG (sbp, mode);
  marshall_LONG (sbp, classid);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);  /* Update length field */

  /* Initialize the response buffer */
  memset (repbuf, 0, sizeof(repbuf));

  /* Send request */
  c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {

    /* Unmarshall response */
    rbp = repbuf;
    unmarshall_LONG (rbp, *newfile);

    if (statbuf != NULL) {
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

    strcpy (file_uniqueid->server, server);
    if (statbuf == NULL) {
      unmarshall_HYPER (rbp, file_uniqueid->fileid);
    } else {
      file_uniqueid->fileid = statbuf->fileid;
    } 
  } else {
    /* Normally when an error occurs we do not unmarshall any response from the
     * server. In the context of Cns_openx we at least try to unmarshall the
     * fileid of the file. Why? In order to return as much information back to
     * the caller as possible. Take for example, the use-case where a file
     * exists but the user has no access to READ or WRITE to it, here it is
     * still nice to gain access to the fileid for pure logging purposes.
     *
     * Note: In cases where we cannot even connect to the name server we are
     * protected by the initialization of the response buffer to zero's.
     */
    rbp = repbuf;
    strcpy (file_uniqueid->server, server);
    unmarshall_LONG (rbp, *newfile);
    unmarshall_HYPER (rbp, file_uniqueid->fileid);
  }
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
