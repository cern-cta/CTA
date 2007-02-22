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


int Cupv_check(uid_t priv_uid, gid_t priv_gid, const char *src, const char *tgt, int priv)
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
	int lensrc, lentgt;

        strcpy (func, "Cupv_check");
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

	if (priv_uid < 0 || priv_gid < 0 || priv < 0) {
	  serrno = EINVAL;
	  return (-1);
	}

	if (src == NULL) {
	  lensrc = 0;
	} else {
	  lensrc = strlen(src);
	}

	if (tgt == NULL) {
	  lentgt = 0;
	} else {
	  lentgt = strlen(tgt);
	}

	if (lensrc > CA_MAXREGEXPLEN || lentgt > CA_MAXREGEXPLEN) {
	  serrno = EINVAL;
	  return(-1);
	}

	/* Applying a first check to see if the request is for root */
	/* In this case just return without asking the server */

	if (priv_uid == 0) {
	  if (src == NULL && tgt == NULL) {
	    /* Both NULL, authorized */
	    return(0);
	  } else if ((src != NULL) && (tgt != NULL) && (strcmp(src, tgt)==0)) {
	    /* src == tmp */
	    return(0);
	  }
	} /* In other cases, a message is sent to the server for validation */

#ifndef USE_CUPV
	serrno = EPERM;
	return(-1);
#else
 	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, CUPV_MAGIC);
	marshall_LONG (sbp, CUPV_CHECK);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG(sbp, msglen);

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, priv_uid);
	marshall_LONG (sbp, priv_gid);

	if (src == NULL) {
	  marshall_STRING (sbp, "");
	} else {
	  marshall_STRING(sbp, src);
	}

	if (tgt == NULL) {
	  marshall_STRING (sbp, "");
	} else {
	  marshall_STRING(sbp, tgt);
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



