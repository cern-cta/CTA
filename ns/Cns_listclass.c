/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)Cns_listclass.c,v 1.3 2001/01/23 11:03:42 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_listclass - list fileclass entries */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

struct Cns_fileclass *
Cns_listclass(char *server, int flags, Cns_list *listp)
{
	int bol = 0;
	int c;
	char func[16];
	gid_t gid;
	int listentsz = sizeof(struct Cns_fileclass);
	struct Cns_fileclass *lp;
	int msglen;
	int nbentries;
	int nbtppools;
	char *p;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;

	strcpy (func, "Cns_listclass");
	if (Cns_apiinit (&thip))
		return (NULL);
	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (NULL);
	}
#endif

	if (! listp) {
		serrno = EFAULT;
		return (NULL);
	}

	if (flags == CNS_LIST_BEGIN) {
		memset (listp, 0, sizeof(Cns_list));
		listp->fd = -1;
		if ((listp->buf = malloc (LISTBUFSZ)) == NULL) {
			serrno = ENOMEM;
			return (NULL);
		}
		bol = 1;
	}
	if (listp->len == 0 && listp->eol	/* all entries have been listed */
	    && flags != CNS_LIST_END)
		return (NULL);

	if (listp->len == 0	/* no data in the cache */
	    || flags == CNS_LIST_END) {

		/* Build request header */

		sbp = sendbuf;
		marshall_LONG (sbp, CNS_MAGIC);
		if (flags == CNS_LIST_END) {
			marshall_LONG (sbp, CNS_ENDLIST);
		} else {
			marshall_LONG (sbp, CNS_LISTCLASS);
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

		while ((c = send2nsd (&listp->fd, server, sendbuf, msglen,
		    repbuf, sizeof(repbuf))) && serrno == ENSNACT)
			sleep (RETRYI);
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
			return (NULL);		/* end of list */

		/* unmarshall reply into Cns_fileclass structures */

		lp = (struct Cns_fileclass *) listp->buf;
		while (nbentries--) {
			unmarshall_LONG (rbp, lp->classid);
			unmarshall_STRING (rbp, lp->name);
			unmarshall_LONG (rbp, lp->uid);
			unmarshall_LONG (rbp, lp->gid);
			unmarshall_LONG (rbp, lp->min_filesize);
			unmarshall_LONG (rbp, lp->max_filesize);
			unmarshall_LONG (rbp, lp->flags);
			unmarshall_LONG (rbp, lp->maxdrives);
			unmarshall_LONG (rbp, lp->max_segsize);
			unmarshall_LONG (rbp, lp->migr_time_interval);
			unmarshall_LONG (rbp, lp->mintime_beforemigr);
			unmarshall_LONG (rbp, lp->nbcopies);
			unmarshall_LONG (rbp, lp->retenp_on_disk);
			unmarshall_LONG (rbp, lp->nbtppools);
			nbtppools = lp->nbtppools;
			lp->tppools = (char *) lp + listentsz;
			p = lp->tppools;
			while (nbtppools--) {
				unmarshall_STRING (rbp, p);
				p += (CA_MAXPOOLNAMELEN+1);
			}
			lp = (struct Cns_fileclass *)
			    (lp->tppools + lp->nbtppools * (CA_MAXCLASNAMELEN+1));
		}
		unmarshall_WORD (rbp, listp->eol);
		listp->len = (char *) lp - listp->buf;
	}
	lp = (struct Cns_fileclass *) (listp->buf + listp->offset);
	listp->offset += listentsz + lp->nbtppools * (CA_MAXCLASNAMELEN+1);
	if (listp->offset >= listp->len) {	/* must refill next time */
		listp->offset = 0;
		listp->len = 0;
	}
	return (lp);
}
