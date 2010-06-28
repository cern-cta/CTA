/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_readdir - read a directory entry */

#include <errno.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

struct dirent DLL_DECL *
Cns_readdir(Cns_DIR *dirp)
{
  int c;
  int direntsz;
  struct dirent *dp;
  char func[16];
  int getattr = 0;
  gid_t gid;
  int msglen;
  int n;
  int nbentries;
  char *q;
  char *rbp;
  char repbuf[DIRBUFSZ+4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  uid_t uid;

  strcpy (func, "Cns_readdir");
  Cns_getid(&uid, &gid);

  if (! dirp) {
    serrno = EFAULT;
    return (NULL);
  }
  /* compute size of client machine dirent structure excluding d_name */

  dp = (struct dirent *) dirp->dd_buf;
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

    /* unmarshall reply into the dirent structures */

    dp = (struct dirent *) dirp->dd_buf;
    while (nbentries--) {
      dp->d_ino = 0;
#if defined(linux)
      dp->d_off = 0;
      dp->d_type = 0;
#endif
      unmarshall_STRING (rbp, dp->d_name);
      n = strlen (dp->d_name);
      dp->d_reclen = ((direntsz + n + 8) / 8) * 8;
      dp = (struct dirent *) ((char *) dp + dp->d_reclen);
    }
    dirp->bod = 0;
    unmarshall_WORD (rbp, dirp->eod);
    dirp->dd_size = (char *) dp - dirp->dd_buf;
  }
  dp = (struct dirent *) (dirp->dd_buf + dirp->dd_loc);
  dirp->dd_loc += dp->d_reclen;
  if (dirp->dd_loc >= dirp->dd_size) { /* must refill next time */
    dirp->dd_loc = 0;
    dirp->dd_size = 0;
  }
  return (dp);
}
