/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cupv_modify.c,v $ $Revision: 1.3 $ $Date: 2002/06/10 13:04:09 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */
 

#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "Cupv_api.h"
#include "Cupv.h"
#include "serrno.h"


Cupv_modify(uid_t priv_uid, gid_t priv_gid, const char *src, const char *tgt,
	    const char *newsrc, const char *newtgt, int priv)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cupv_api_thread_info *thip;
	uid_t uid;
	int lensrc, lentgt, lennewsrc, lennewtgt;

        strcpy (func, "Cupv_modify");
        if (Cupv_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();
#if defined(_WIN32)
        if (uid < 0 || gid < 0) {
                Cupv_errmsg (func, CUP53);
                serrno = SENOMAPFND;
                return (-1);
        }
#endif


	lensrc = strlen(src);
	lentgt = strlen(tgt);
	lennewsrc = strlen(newsrc);
	lennewtgt = strlen(newtgt);

	if (src == NULL || tgt == NULL
	    || lensrc == 0 || lentgt == 0 || lennewsrc == 0 || lennewtgt == 0
	    || lensrc > CA_MAXREGEXPLEN || lentgt > CA_MAXREGEXPLEN
	    || lennewsrc > CA_MAXREGEXPLEN || lennewtgt > CA_MAXREGEXPLEN) {
		serrno = EINVAL;
		return (-1);
	}
 

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, CUPV_MAGIC);
	marshall_LONG (sbp, CUPV_MODIFY);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG(sbp, msglen);

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, priv_uid);
	marshall_LONG (sbp, priv_gid);
	marshall_STRING (sbp, src);
	marshall_STRING (sbp, tgt);
	marshall_STRING (sbp, newsrc);
	marshall_STRING (sbp, newtgt);
	marshall_LONG (sbp, priv);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2Cupv (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == ECUPVNACT)
		sleep (RETRYI);
	return (c);
}





