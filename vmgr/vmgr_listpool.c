/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_listpool - list tape pool entries */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "serrno.h"
#include "vmgr_api.h"
#include "vmgr.h"

struct vmgr_tape_pool_byte_u64 *
vmgr_listpool_byte_u64(int flags, vmgr_list *listp)
{
	int bol = 0;
	int c;
	char func[14];
	gid_t gid;
	int listentsz = sizeof(struct vmgr_tape_pool_byte_u64);
	struct vmgr_tape_pool_byte_u64 *lp = NULL;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strncpy (func, "vmgr_listpool", 14);
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
		marshall_LONG (sbp, VMGR_MAGIC);
		if (flags == VMGR_LIST_END) {
			marshall_LONG (sbp, VMGR_ENDLIST);
		} else {
			marshall_LONG (sbp, VMGR_LISTPOOL);
		}
		q = sbp;        /* save pointer. The next field will be updated */
		msglen = 3 * LONGSIZE;
		marshall_LONG (sbp, msglen);

		/* Build request body */
	 
		marshall_LONG (sbp, uid);
		marshall_LONG (sbp, gid);
		marshall_WORD (sbp, listentsz);
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

		/* unmarshall reply into vmgr_tape_pool structures */

		listp->nbentries = nbentries;
		lp = (struct vmgr_tape_pool_byte_u64 *) listp->buf;
		while (nbentries--) {
			unmarshall_STRING (rbp, lp->name);
			unmarshall_LONG (rbp, lp->uid);
			unmarshall_LONG (rbp, lp->gid);
			{
			  u_signed64 capacity_kibibyte_u64 = 0;
			  unmarshall_HYPER (rbp, capacity_kibibyte_u64);
			  lp->capacity_byte_u64 = capacity_kibibyte_u64 *
			    ONE_KB;
			}
			{
			  u_signed64 tot_free_space_kibibyte_u64 = 0;
			  unmarshall_HYPER (rbp, tot_free_space_kibibyte_u64);
			  lp->tot_free_space_byte_u64 =
			    tot_free_space_kibibyte_u64 * ONE_KB;
			}
			lp++;
		}
		unmarshall_WORD (rbp, listp->eol);
	}
	lp = ((struct vmgr_tape_pool_byte_u64 *) listp->buf) + listp->index;
	listp->index++;
	if (listp->index >= listp->nbentries) {	/* must refill next time */
		listp->index = 0;
		listp->nbentries = 0;
	}
	return (lp);
}
