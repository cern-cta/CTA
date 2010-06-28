/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_listlibrary - list all existing tape libraries */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "serrno.h"
#include "vmgr_api.h"
#include "vmgr.h"

struct vmgr_tape_library *
vmgr_listlibrary(int flags, vmgr_list *listp)
{
	int bol = 0;
	int c;
	char func[17];
	gid_t gid;
	int listentsz = sizeof(struct vmgr_tape_library);
	struct vmgr_tape_library *lp;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strcpy (func, "vmgr_listlibrary");
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
			marshall_LONG (sbp, VMGR_LISTLIBRARY);
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

		/* unmarshall reply into vmgr_tape_library structures */

		listp->nbentries = nbentries;
		lp = (struct vmgr_tape_library *) listp->buf;
		while (nbentries--) {
			unmarshall_STRING (rbp, lp->name);
			unmarshall_LONG (rbp, lp->capacity);
			unmarshall_LONG (rbp, lp->nb_free_slots);
			unmarshall_LONG (rbp, lp->status);
			lp++;
		}
		unmarshall_WORD (rbp, listp->eol);
	}
	lp = ((struct vmgr_tape_library *) listp->buf) + listp->index;
	listp->index++;
	if (listp->index >= listp->nbentries) {	/* must refill next time */
		listp->index = 0;
		listp->nbentries = 0;
	}
	return (lp);
}
