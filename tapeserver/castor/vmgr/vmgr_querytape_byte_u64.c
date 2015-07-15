/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgr_querytape_byte_u64 - query about a tape volume */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"

int vmgr_querytape_byte_u64(const char *vid, int side,
                            struct vmgr_tape_info_byte_u64 *tape_info, char *dgn)
{
  int c;
  char func[15];
  gid_t gid;
  int msglen;
  char *q;
  char *rbp;
  char repbuf[177];
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct vmgr_api_thread_info *thip;
  char tmpbuf[7];
  uid_t uid;

  strncpy (func, "vmgr_querytape", 15);
  if (vmgr_apiinit (&thip))
    return (-1);
  uid = geteuid();
  gid = getegid();

  if (! vid || ! tape_info) {
    serrno = EFAULT;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, VMGR_MAGIC2);
  marshall_LONG (sbp, VMGR_QRYTAPE_BYTE_U64);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_STRING (sbp, vid);
  marshall_WORD (sbp, side);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while ((c = send2vmgr (NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
         serrno == EVMGRNACT)
    sleep (RETRYI);
  if (c == 0) {
    rbp = repbuf;
    strcpy (tape_info->vid, vid);
    unmarshall_STRING (rbp, tape_info->vsn);
    unmarshall_STRING (rbp, tape_info->library);
    unmarshall_STRING (rbp, tmpbuf);
    if (dgn)
      strcpy (dgn, tmpbuf);
    unmarshall_STRING (rbp, tape_info->density);
    unmarshall_STRING (rbp, tape_info->lbltype);
    unmarshall_STRING (rbp, tape_info->model);
    unmarshall_STRING (rbp, tape_info->media_letter);
    unmarshall_STRING (rbp, tape_info->manufacturer);
    unmarshall_STRING (rbp, tape_info->sn);
    unmarshall_WORD (rbp, tape_info->nbsides);
    unmarshall_TIME_T (rbp, tape_info->etime);
    unmarshall_WORD (rbp, tape_info->side);
    unmarshall_STRING (rbp, tape_info->poolname);
    unmarshall_HYPER (rbp,
                      tape_info->estimated_free_space_byte_u64);
    unmarshall_LONG (rbp, tape_info->nbfiles);
    unmarshall_LONG (rbp, tape_info->rcount);
    unmarshall_LONG (rbp, tape_info->wcount);
    unmarshall_STRING (rbp, tape_info->rhost);
    unmarshall_STRING (rbp, tape_info->whost);
    unmarshall_LONG (rbp, tape_info->rjid);
    unmarshall_LONG (rbp, tape_info->wjid);
    unmarshall_TIME_T (rbp, tape_info->rtime);
    unmarshall_TIME_T (rbp, tape_info->wtime);
    unmarshall_LONG (rbp, tape_info->status);
  }
  return (c);
}
