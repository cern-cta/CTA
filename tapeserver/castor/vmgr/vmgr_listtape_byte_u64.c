/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      vmgr_listtape_byte_u64 - list all existing tapes */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "serrno.h"
#include "vmgr_api.h"
#include "vmgr.h"

struct vmgr_tape_info_byte_u64 *
vmgr_listtape_byte_u64(char *vid, char *pool_name, int flags, vmgr_list *listp)
{
  int bol = 0;
  int c;
  char func[14];
  gid_t gid;

  /* Please note that the size of the vmgr_tape_info_byte_u64        */
  /* structure is larger than what is actually required.   The       */
  /* vmgr_tape_info_byte_u64 structure stores                        */
  /* estimated_free_space_byte_u64 as a u_signed64. The old vmgr     */
  /* TCP/IP protocol using VMGR_MAGIC2 and VMGR_LISTTAPE marshalls a */
  /* signed 32-bit integer it represent the estimated free-space in  */
  /* kibibytes.                                                      */
  int listentsz = sizeof(struct vmgr_tape_info_byte_u64);

  struct vmgr_tape_info_byte_u64 *lp = NULL;
  int msglen;
  int nbentries;
  char *q;
  char *rbp;
  char repbuf[LISTBUFSZ+4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct vmgr_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "vmgr_listtape", 14);
  if (vmgr_apiinit (&thip))
    return (NULL);
  uid = geteuid();
  gid = getegid();

  if (! listp) {
    serrno = EFAULT;
    return (NULL);
  }

  if (flags == VMGR_LIST_BEGIN) {
    memset (listp, 0, sizeof(vmgr_list));
    listp->fd = -1;
    if ((listp->buf = malloc (LISTBUFSZ)) == NULL) {
      serrno = ENOMEM;
      return (NULL);
    }
    bol = 1;
  }
  if (listp->nbentries == 0 && listp->eol	/* all entries have been listed */
      && flags != VMGR_LIST_END)
    return (NULL);

  if (listp->nbentries == 0	/* no data in the cache */
      || flags == VMGR_LIST_END) {

    /* Build request header */

    sbp = sendbuf;
    marshall_LONG (sbp, VMGR_MAGIC2);
    if (flags == VMGR_LIST_END) {
      marshall_LONG (sbp, VMGR_ENDLIST);
    } else {
      marshall_LONG (sbp, VMGR_LISTTAPE_BYTE_U64);
    }
    q = sbp;        /* save pointer. The next field will be updated */
    msglen = 3 * LONGSIZE;
    marshall_LONG (sbp, msglen);

    /* Build request body */

    marshall_LONG (sbp, uid);
    marshall_LONG (sbp, gid);
    marshall_WORD (sbp, listentsz);
    if (vid) {
      marshall_STRING (sbp, vid);
    } else {
      marshall_STRING (sbp, "");
    }
    if (pool_name) {
      marshall_STRING (sbp, pool_name);
    } else {
      marshall_STRING (sbp, "");
    }
    marshall_WORD (sbp, bol);

    msglen = sbp - sendbuf;
    marshall_LONG (q, msglen);	/* update length field */

    while ((c = send2vmgr (&listp->fd, sendbuf, msglen,
                           repbuf, sizeof(repbuf))) && serrno == EVMGRNACT)
      sleep (RETRYI);
    if (c < 0)
      return (NULL);
    if (flags == VMGR_LIST_END) {
      if (listp->buf)
        free (listp->buf);
      return (NULL);
    }
    rbp = repbuf;
    unmarshall_WORD (rbp, nbentries);
    if (nbentries == 0)
      return (NULL);		/* end of list */

    /* unmarshall reply into vmgr_tape_info structures */

    listp->nbentries = nbentries;
    lp = (struct vmgr_tape_info_byte_u64 *) listp->buf;
    while (nbentries--) {
      unmarshall_STRING (rbp, lp->vid);
      unmarshall_STRING (rbp, lp->vsn);
      unmarshall_STRING (rbp, lp->library);
      unmarshall_STRING (rbp, lp->density);
      unmarshall_STRING (rbp, lp->lbltype);
      unmarshall_STRING (rbp, lp->model);
      unmarshall_STRING (rbp, lp->media_letter);
      unmarshall_STRING (rbp, lp->manufacturer);
      unmarshall_STRING (rbp, lp->sn);
      unmarshall_WORD (rbp, lp->nbsides);
      unmarshall_TIME_T (rbp, lp->etime);
      unmarshall_WORD (rbp, lp->side);
      unmarshall_STRING (rbp, lp->poolname);
      unmarshall_HYPER (rbp,
                        lp->estimated_free_space_byte_u64);
      unmarshall_LONG (rbp, lp->nbfiles);
      unmarshall_LONG (rbp, lp->rcount);
      unmarshall_LONG (rbp, lp->wcount);
      unmarshall_STRING (rbp, lp->rhost);
      unmarshall_STRING (rbp, lp->whost);
      unmarshall_LONG (rbp, lp->rjid);
      unmarshall_LONG (rbp, lp->wjid);
      unmarshall_TIME_T (rbp, lp->rtime);
      unmarshall_TIME_T (rbp, lp->wtime);
      unmarshall_LONG (rbp, lp->status);
      lp++;
    }
    unmarshall_WORD (rbp, listp->eol);
  }
  lp = ((struct vmgr_tape_info_byte_u64 *) listp->buf) + listp->index;
  listp->index++;
  if (listp->index >= listp->nbentries) {	/* must refill next time */
    listp->index = 0;
    listp->nbentries = 0;
  }
  return (lp);
}
