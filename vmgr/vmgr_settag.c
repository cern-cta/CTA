/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

/*	vmgr_settag - add/replace a tag associated with a tape volume */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"

int
vmgr_settag(const char *vid, char *tag)
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct vmgr_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "vmgr_settag", 16);
  if (vmgr_apiinit (&thip))
    return (-1);
  uid = geteuid();
  gid = getegid();

  if (! vid || ! tag) {
    serrno = EFAULT;
    return (-1);
  }

  if (strlen (vid) > CA_MAXVIDLEN) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, VMGR_MAGIC);
  marshall_LONG (sbp, VMGR_SETTAG);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_STRING (sbp, vid);
  marshall_STRING (sbp, tag);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
         serrno == EVMGRNACT)
    sleep (RETRYI);
  return (c);
}
