/*
 * Copyright (C) 2005 by CERN/IT/GD/CT
 * All rights reserved
 */

/*	Cns_listreplicax - list replica entries for a given pool/server/filesystem */

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

struct Cns_filereplica *
Cns_listreplicax(const char *poolname, const char *server, const char *fs, int flags, Cns_list *listp)
{
	int bol = 0;
	int c;
	char func[17];
	gid_t gid;
	int listentsz;
	struct Cns_filereplica *lp;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;

	strcpy (func, "Cns_listreplicax");
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

	if ((! poolname && ! server && ! fs) || ! listp) {
		serrno = EFAULT;
		return (NULL);
	}

	if ((poolname && strlen (poolname) > CA_MAXPOOLNAMELEN) ||
	    (server && strlen (server) > CA_MAXHOSTNAMELEN) ||
	    (fs && strlen (fs)) > 79) {
		serrno = EINVAL;
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
	if (listp->buf == NULL)			/* buffer has already been freed
						   and connection closed */
		return (NULL);

	/* compute size of Cns_filereplica structure excluding sfn */

	lp = (struct Cns_filereplica *) listp->buf;
	listentsz = &lp->sfn[0] - (char *) lp;

	if (listp->len == 0	/* no data in the cache */
	    || flags == CNS_LIST_END) {

		/* Build request header */

		sbp = sendbuf;
		marshall_LONG (sbp, CNS_MAGIC);
		if (flags == CNS_LIST_END) {
			marshall_LONG (sbp, CNS_ENDLIST);
		} else {
			marshall_LONG (sbp, CNS_LISTREPLICAX);
		}
		q = sbp;        /* save pointer. The next field will be updated */
		msglen = 3 * LONGSIZE;
		marshall_LONG (sbp, msglen);

		/* Build request body */
	 
		marshall_LONG (sbp, uid);
		marshall_LONG (sbp, gid);
		marshall_WORD (sbp, listentsz);
		if (poolname) {
			marshall_STRING (sbp, poolname);
		} else {
			marshall_STRING (sbp, "");
		}
		if (server) {
			marshall_STRING (sbp, server);
		} else {
			marshall_STRING (sbp, "");
		}
		if (fs) {
			marshall_STRING (sbp, fs);
		} else {
			marshall_STRING (sbp, "");
		}
		marshall_WORD (sbp, bol);
	 
		msglen = sbp - sendbuf;
		marshall_LONG (q, msglen);	/* update length field */

		while ((c = send2nsd (&listp->fd, NULL, sendbuf,
		    msglen, repbuf, sizeof(repbuf))) && serrno == ENSNACT)
			sleep (RETRYI);
		if (c < 0 || flags == CNS_LIST_END) {
			if (listp->buf)
				free (listp->buf);
			listp->buf = NULL;
			return (NULL);
		}
		rbp = repbuf;
		unmarshall_WORD (rbp, nbentries);
		if (nbentries == 0)
			return (NULL);		/* end of list */

		/* unmarshall reply into Cns_filereplica structures */

		while (nbentries--) {
			unmarshall_HYPER (rbp, lp->fileid);
			unmarshall_HYPER (rbp, lp->nbaccesses);
			unmarshall_TIME_T (rbp, lp->atime);
			unmarshall_TIME_T (rbp, lp->ptime);
			unmarshall_BYTE (rbp, lp->status);
			unmarshall_BYTE (rbp, lp->f_type);
			unmarshall_STRING (rbp, lp->poolname);
			unmarshall_STRING (rbp, lp->host);
			unmarshall_STRING (rbp, lp->fs);
			unmarshall_STRING (rbp, lp->sfn);
			c = ((listentsz + strlen (lp->sfn) + 8) / 8) * 8;
			lp = (struct Cns_filereplica *) ((char *) lp + c);
		}
		unmarshall_WORD (rbp, listp->eol);
		listp->len = (char *) lp - listp->buf;
	}
	lp = (struct Cns_filereplica *) (listp->buf + listp->offset);
	listp->offset += ((listentsz + strlen (lp->sfn) + 8) / 8) * 8;
	if (listp->offset >= listp->len) {	/* must refill next time */
		listp->offset = 0;
		listp->len = 0;
	}
	return (lp);
}
