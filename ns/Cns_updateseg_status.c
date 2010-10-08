/*
 * Copyright (C) 2002 by CERN/IT/DM
 * All rights reserved
 */

/* Cns_updateseg_status - updates the segment status */

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
Cns_updateseg_status(char *server, u_signed64 fileid, struct Cns_segattrs *oldsegattrs, const char status)
{
  int c;
  char func[21];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "Cns_updateseg_status", 21);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! oldsegattrs) {
    serrno = EFAULT;
    return (-1);
  }

  if (! oldsegattrs->vid) {
    serrno = EFAULT;
    return (-1);
  }
  if (strlen(oldsegattrs->vid) > CA_MAXVIDLEN) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_UPDATESEG_STATUS);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, fileid);
  marshall_WORD (sbp, oldsegattrs->copyno);
  marshall_WORD (sbp, oldsegattrs->fsec);

  marshall_STRING (sbp, oldsegattrs->vid);
  marshall_WORD (sbp, oldsegattrs->side);
  marshall_LONG (sbp, oldsegattrs->fseq);

  marshall_BYTE (sbp, oldsegattrs->s_status);
  marshall_BYTE (sbp, status);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  return (c);
}
