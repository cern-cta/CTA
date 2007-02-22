/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
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


int Cupv_modify(uid_t priv_uid, gid_t priv_gid, const char *src, const char *tgt,
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

	if (priv_uid < 0 || priv_gid < 0) {
	  serrno = EINVAL;
	  return(-1);
	} 

	/* Checking the stings passed ... */

	if (src != NULL) {
	  lensrc = strlen(src);
	} else {
	  lensrc = 0;
	}
	
	if (tgt != NULL) {
	  lentgt = strlen(tgt);
	} else { 
	  lentgt = 0;
	}
	if (newsrc != NULL) {
	  lennewsrc = strlen(newsrc);
	} else {
	  lennewsrc = 0;
	}
	
	if (newtgt != NULL) {
	  lennewtgt = strlen(newtgt);
	} else { 
	  lennewtgt = 0;
	}

	if (lensrc == 0 || lentgt == 0 
	    || lensrc > CA_MAXREGEXPLEN || lentgt > CA_MAXREGEXPLEN
	    || lennewsrc > CA_MAXREGEXPLEN || lennewtgt > CA_MAXREGEXPLEN) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* At least one of priv, srchost and tgthost should be non null */
	if (lennewsrc == 0 && lennewtgt == 0 && priv == -1) {
	  serrno = EINVAL;
	  return(-1);
	}
#ifndef USE_CUPV
	serrno = EPERM;
	return(-1);
#else
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
	if (newsrc != NULL) {
	  marshall_STRING (sbp, newsrc);
	} else {
	  marshall_STRING (sbp, "");
	}
	if (newtgt != NULL) {
	  marshall_STRING (sbp, newtgt);
	} else {
	  marshall_STRING (sbp, "");
	}
	marshall_LONG (sbp, priv);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2Cupv (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == ECUPVNACT)
		sleep (RETRYI);
	return (c);
#endif
}





