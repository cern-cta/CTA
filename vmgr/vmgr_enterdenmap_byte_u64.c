/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgr_enterdenmap_byte_u64 - enter a new quadruplet model/media_letter/density/capacity */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"
#include <string.h>

int vmgr_enterdenmap_byte_u64(const char *model, char *media_letter,
                              char *density, const u_signed64 native_capacity_byte_u64)
{
  int c;
  char func[17];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct vmgr_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "vmgr_enterdenmap", 17);
  if (vmgr_apiinit (&thip))
    return (-1);
  uid = geteuid();
  gid = getegid();

  if (! model || ! density) {
    serrno = EFAULT;
    return (-1);
  }

  if (strlen (model) > CA_MAXMODELLEN ||
      (media_letter && strlen (media_letter) > CA_MAXMLLEN) ||
      strlen (density) > CA_MAXDENLEN) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, VMGR_MAGIC2);
  marshall_LONG (sbp, VMGR_ENTDENMAP_BYTE_U64);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_STRING (sbp, model);
  if (media_letter) {
    marshall_STRING (sbp, media_letter);
  } else {
    marshall_STRING (sbp, " ");
  }
  marshall_STRING (sbp, density);
  marshall_HYPER (sbp, native_capacity_byte_u64);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
         serrno == EVMGRNACT)
    sleep (RETRYI);
  return (c);
}
