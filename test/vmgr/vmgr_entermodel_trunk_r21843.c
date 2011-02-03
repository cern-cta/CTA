/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_entermodel - enter a new model of cartridge */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include <string.h>

int vmgr_entermodel(const char *model, char *media_letter, int media_cost)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strncpy (func, "vmgr_entermodel", 16);
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
	marshall_LONG (sbp, VMGR_ENTMODEL);
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
	marshall_LONG (sbp, media_cost);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
