/*
 * Copyright (C) 2000 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
/*      Cupv_list - list privileges */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "Cupv_api.h"
#include "Cupv.h"

struct Cupv_userpriv *
Cupv_list(int flags, Cupv_entry_list *listp, struct Cupv_userpriv *filter)
{
	int bol = 0;
	int c;
	char func[15];
	gid_t gid;
	int listentsz = sizeof(struct Cupv_userpriv);
	struct Cupv_userpriv *lp;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cupv_api_thread_info *thip;
	uid_t uid;

        strcpy (func, "Cupv_list");
        if (Cupv_apiinit (&thip))
                return (NULL);
        uid = geteuid();
        gid = getegid();
#if defined(_WIN32)
        if (uid < 0 || gid < 0) {
                Cupv_errmsg (func, CUP53);
                serrno = SENOMAPFND;
                return (NULL);
        }
#endif

	if (strlen(filter->srchost) > CA_MAXREGEXPLEN 
	    || strlen(filter->tgthost) > CA_MAXREGEXPLEN) {
		serrno = EINVAL;
		return (NULL);
	}

	if (! listp) {
		serrno = EFAULT;
		return (NULL);
	}

	if (flags == CUPV_LIST_BEGIN) {
		memset (listp, 0, sizeof(Cupv_entry_list));
		listp->fd = -1;
		if ((listp->buf = malloc (LISTBUFSZ)) == NULL) {
			serrno = ENOMEM;
			return (NULL);
		}
		bol = 1;
	}
	if (listp->nbentries == 0 && listp->eol	/* all entries have been listed */
	    && flags != CUPV_LIST_END)
		return (NULL);

	if (listp->nbentries == 0	/* no data in the cache */
	    || flags == CUPV_LIST_END) {

		/* Build request header */

		sbp = sendbuf;
		marshall_LONG (sbp, CUPV_MAGIC);
		if (flags == CUPV_LIST_END) {
			marshall_LONG (sbp, CUPV_ENDLIST);
		} else {
			marshall_LONG (sbp, CUPV_LIST);
		}
		q = sbp;        /* save pointer. The next field will be updated */
		msglen = 3 * LONGSIZE;
		marshall_LONG (sbp, msglen);

		/* Build request body */
	 
		marshall_LONG (sbp, uid);
		marshall_LONG (sbp, gid);
		marshall_WORD (sbp, listentsz); 
		marshall_WORD (sbp, bol);

		marshall_LONG (sbp, filter->uid);
		marshall_LONG (sbp, filter->gid);
		marshall_STRING (sbp, filter->srchost);
		marshall_STRING (sbp, filter->tgthost);
		marshall_LONG (sbp, filter->privcat);
	 
		msglen = sbp - sendbuf;
		marshall_LONG (q, msglen);	/* update length field */


		while ((c = send2Cupv (&listp->fd, sendbuf, msglen,
		    repbuf, sizeof(repbuf))) && serrno == ECUPVNACT)
			sleep (RETRYI);
		if (c < 0)
			return (NULL);
		if (flags == CUPV_LIST_END) {
			if (listp->buf)
				free (listp->buf);
			return (NULL);
		}
		rbp = repbuf;
		unmarshall_WORD (rbp, nbentries);
		if (nbentries == 0)
			return (NULL);		/* end of list */

		/* unmarshall reply into Cupv_userpriv structures */

		listp->nbentries = nbentries;
		lp = (struct Cupv_userpriv *) listp->buf;
		while (nbentries--) {
			unmarshall_LONG (rbp, lp->uid);
			unmarshall_LONG (rbp, lp->gid);
			unmarshall_STRINGN (rbp, lp->srchost, CA_MAXREGEXPLEN);
			unmarshall_STRINGN (rbp, lp->tgthost, CA_MAXREGEXPLEN);
			unmarshall_LONG (rbp, lp->privcat);
			lp++;
		}
		unmarshall_WORD (rbp, listp->eol);
	}
	lp = ((struct Cupv_userpriv *) listp->buf) + listp->index;
	listp->index++;
	if (listp->index >= listp->nbentries) {	/* must refill next time */
		listp->index = 0;
		listp->nbentries = 0;
	}
	return (lp);
}
