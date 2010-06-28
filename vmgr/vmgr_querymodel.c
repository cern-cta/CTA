/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_querymodel - query about a model of cartridge */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"
#include <string.h>

int vmgr_querymodel(const char *model, char *media_letter, int *media_cost)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	int n;
	char *q;
	char *rbp;
	char repbuf[11];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	char tmpbuf[7];
	uid_t uid;

        strcpy (func, "vmgr_querymodel");
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();

	if (! model) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (model) > CA_MAXMODELLEN ||
	    (media_letter && strlen (media_letter) > CA_MAXMLLEN)) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC2);
	marshall_LONG (sbp, VMGR_QRYMODEL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, model);
	if (media_letter) {
		marshall_STRING (sbp, media_letter);
	} else {
		marshall_STRING (sbp, " ");
	}
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_STRING (rbp, tmpbuf);
		if (media_letter)
			strcpy (media_letter, tmpbuf);
		unmarshall_LONG (rbp, n);
		if (media_cost)
			*media_cost = n;
	}
	return (c);
}
