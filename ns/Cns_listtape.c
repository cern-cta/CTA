/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_listtape - list the file segments residing on a volume */

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

struct Cns_direntape *
Cns_listtape(char *server, char *vid, int flags, Cns_list *listp, int fseq)
{
  int bov = 0;
  int c;
  int direntsz;
  struct Cns_direntape *dp;
  char func[16];
  gid_t gid;
  int msglen;
  int nbentries;
  char *q;
  char *rbp;
  char repbuf[DIRBUFSZ+4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  uid_t uid;
  struct Cns_api_thread_info *thip;

  strncpy (func, "Cns_listtape", 16);
  if (Cns_apiinit (&thip))
    return (NULL);
  Cns_getid(&uid, &gid);

  if (! vid || ! listp) {
    serrno = EFAULT;
    return (NULL);
  }
  if (strlen (vid) > CA_MAXVIDLEN) {
    serrno = EINVAL;
    return (NULL);
  }

  if (flags == CNS_LIST_BEGIN) {
    memset (listp, 0, sizeof(Cns_list));
    listp->fd = -1;
    if ((listp->buf = malloc (DIRBUFSZ)) == NULL) {
      serrno = ENOMEM;
      return (NULL);
    }
    bov = 1;
  }
  if (listp->len == 0 && listp->eol /* all entries have been listed */
      && flags != CNS_LIST_END)
    return (NULL);

  /* compute size of client machine Cns_direntape structure excluding d_name */

  dp = (struct Cns_direntape *) listp->buf;
  direntsz = &dp->d_name[0] - (char *) dp;
  if (listp->len == 0 /* no data in the cache */
      || flags == CNS_LIST_END) {

    /* Build request header */

    sbp = sendbuf;
    marshall_LONG (sbp, CNS_MAGIC5);
    if (flags == CNS_LIST_END) {
      marshall_LONG (sbp, CNS_ENDLIST);
    } else {
      marshall_LONG (sbp, CNS_LISTTAPE);
    }
    q = sbp;        /* save pointer. The next field will be updated */
    msglen = 3 * LONGSIZE;
    marshall_LONG (sbp, msglen);

    /* Build request body */

    marshall_LONG (sbp, uid);
    marshall_LONG (sbp, gid);
    marshall_WORD (sbp, direntsz);
    marshall_STRING (sbp, vid);
    marshall_WORD (sbp, bov);
    marshall_LONG (sbp, fseq);

    msglen = sbp - sendbuf;
    marshall_LONG (q, msglen); /* update length field */

    c = send2nsd (&listp->fd, server, sendbuf, msglen,
                  repbuf, sizeof(repbuf));

    if (c < 0)
      return (NULL);
    if (flags == CNS_LIST_END) {
      if (listp->buf)
        free (listp->buf);
      return (NULL);
    }
    rbp = repbuf;
    unmarshall_WORD (rbp, nbentries);
    if (nbentries == 0)
      return (NULL);  /* end of volume */

    /* unmarshall reply into the Cns_direntape structures */

    dp = (struct Cns_direntape *) listp->buf;
    while (nbentries--) {
      unmarshall_HYPER (rbp, dp->parent_fileid);
      unmarshall_HYPER (rbp, dp->fileid);
      unmarshall_WORD (rbp, dp->copyno);
      unmarshall_WORD (rbp, dp->fsec);
      unmarshall_HYPER (rbp, dp->segsize);
      unmarshall_LONG (rbp, dp->compression);
      unmarshall_BYTE (rbp, dp->s_status);
      unmarshall_STRING (rbp, dp->vid);
      unmarshall_STRINGN (rbp, dp->checksum_name,
                          CA_MAXCKSUMNAMELEN);
      unmarshall_LONG (rbp, dp->checksum);
      unmarshall_WORD (rbp, dp->side);
      unmarshall_LONG (rbp, dp->fseq);
      unmarshall_OPAQUE (rbp, dp->blockid, 4);
      unmarshall_STRING (rbp, dp->d_name);

      dp->d_reclen = ((direntsz + strlen (dp->d_name) + 8) / 8) * 8;
      dp = (struct Cns_direntape *) ((char *) dp + dp->d_reclen);
    }
    unmarshall_WORD (rbp, listp->eol);
    listp->len = (char *) dp - listp->buf;
  }
  dp = (struct Cns_direntape *) (listp->buf + listp->offset);
  listp->offset += dp->d_reclen;
  if (listp->offset >= listp->len) { /* must refill next time */
    listp->offset = 0;
    listp->len = 0;
  }
  return (dp);
}
