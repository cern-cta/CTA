/*
 * Copyright (C) 2000-2004 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_lastfseq - return the segment attributes for the last
 * file sequence number residing on a volume.
 */

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

int
Cns_lastfseq(const char *vid, int side, struct Cns_segattrs *segattrs)
{
  /* Variables */
  char  func[16];
  char  sendbuf[REQBUFSZ];
  char  repbuf[REPBUFSZ];
  char  *q;
  char  *sbp;
  char  *rbp;
  int   msglen;
  int   c;
  gid_t gid;
  uid_t uid;
  struct Cns_api_thread_info *thip;

  strcpy (func, "Cns_listfseq");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  /* Check that VID exists and is not too long */
  if (!vid) {
    serrno = EFAULT;
    return (-1);
  }
  if (strlen(vid) > CA_MAXVIDLEN) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build the request header */
  sbp = sendbuf;
  marshall_LONG(sbp, CNS_MAGIC4);
  marshall_LONG(sbp, CNS_LASTFSEQ);
  q = sbp;                   /* Save the pointer, for field updates */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build the request body */
  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_STRING(sbp, vid);
  marshall_LONG(sbp, side);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen);  /* Update the length field */

  /* Send message to name server daemon */
  c = send2nsd(NULL, NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;

    /* Fill Cns_segattrs structure */
    unmarshall_WORD(rbp, segattrs->copyno);
    unmarshall_WORD(rbp, segattrs->fsec);
    unmarshall_HYPER(rbp, segattrs->segsize);
    unmarshall_LONG(rbp, segattrs->compression);
    unmarshall_BYTE(rbp, segattrs->s_status);
    unmarshall_STRING(rbp, segattrs->vid);
    unmarshall_WORD(rbp, segattrs->side);
    unmarshall_LONG(rbp, segattrs->fseq);
    unmarshall_OPAQUE(rbp, segattrs->blockid, 4);
    unmarshall_STRINGN(rbp, segattrs->checksum_name, CA_MAXCKSUMNAMELEN);
    unmarshall_LONG(rbp, segattrs->checksum);
    return (0);
  }
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
