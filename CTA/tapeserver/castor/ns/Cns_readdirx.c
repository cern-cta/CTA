/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_readdirx - read a directory entry including file attributes */

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

struct Cns_direnstat *
Cns_readdirx(Cns_DIR *dirp)
{
  int c;
  int direntsz;
  struct Cns_direnstat *dp;
  char func[16];
  int getattr = 1;
  gid_t gid;
  int msglen;
  int nbentries;
  char *q;
  char *rbp;
  char repbuf[DIRBUFSZ+4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  uid_t uid;

  strncpy (func, "Cns_readdirx", 16);
  Cns_getid(&uid, &gid);

  if (! dirp) {
    serrno = EFAULT;
    return (NULL);
  }
  /* compute size of client machine Cns_direnstat structure excluding d_name */

  dp = (struct Cns_direnstat *) dirp->dd_buf;
  direntsz = &dp->d_name[0] - (char *) dp;

  if (dirp->dd_size == 0) { /* no data in the cache */
    if (dirp->eod)
      return (NULL);

    /* Build request header */

    sbp = sendbuf;
    marshall_LONG (sbp, CNS_MAGIC);
    marshall_LONG (sbp, CNS_READDIR);
    q = sbp;        /* save pointer. The next field will be updated */
    msglen = 3 * LONGSIZE;
    marshall_LONG (sbp, msglen);

    /* Build request body */

    marshall_LONG (sbp, uid);
    marshall_LONG (sbp, gid);
    marshall_WORD (sbp, getattr);
    marshall_WORD (sbp, direntsz);
    marshall_HYPER (sbp, dirp->fileid);
    marshall_WORD (sbp, dirp->bod);

    msglen = sbp - sendbuf;
    marshall_LONG (q, msglen); /* update length field */

    c = send2nsd (&dirp->dd_fd, NULL, sendbuf, msglen,
                  repbuf, sizeof(repbuf));
    if (c < 0)
      return (NULL);
    rbp = repbuf;
    unmarshall_WORD (rbp, nbentries);
    if (nbentries == 0)
      return (NULL);  /* end of directory */

    /* unmarshall reply into the Cns_direnstat structures */

    dp = (struct Cns_direnstat *) dirp->dd_buf;
    while (nbentries--) {
      unmarshall_HYPER (rbp, dp->fileid);
      unmarshall_WORD (rbp, dp->filemode);
      unmarshall_LONG (rbp, dp->nlink);
      unmarshall_LONG (rbp, dp->uid);
      unmarshall_LONG (rbp, dp->gid);
      unmarshall_HYPER (rbp, dp->filesize);
      unmarshall_TIME_T (rbp, dp->atime);
      unmarshall_TIME_T (rbp, dp->mtime);
      unmarshall_TIME_T (rbp, dp->ctime);
      unmarshall_WORD (rbp, dp->fileclass);
      unmarshall_BYTE (rbp, dp->status);
      unmarshall_STRING (rbp, dp->d_name);
      dp->d_reclen = ((direntsz + strlen (dp->d_name) + 8) / 8) * 8;
      dp = (struct Cns_direnstat *) ((char *) dp + dp->d_reclen);
    }
    dirp->bod = 0;
    unmarshall_WORD (rbp, dirp->eod);
    dirp->dd_size = (char *) dp - dirp->dd_buf;
  }
  dp = (struct Cns_direnstat *) (dirp->dd_buf + dirp->dd_loc);
  dirp->dd_loc += dp->d_reclen;
  if (dirp->dd_loc >= dirp->dd_size) { /* must refill next time */
    dirp->dd_loc = 0;
    dirp->dd_size = 0;
  }
  return (dp);
}
