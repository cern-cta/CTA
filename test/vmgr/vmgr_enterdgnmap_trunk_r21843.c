/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_enterdgnmap - enter a new triplet dgn/model/library */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include <string.h>

int vmgr_enterdgnmap(const char *dgn, char *model, char *library)
{
	int c;
	char func[17];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strncpy (func, "vmgr_enterdgnmap", 17);
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();

	if (! dgn || ! model || ! library) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (dgn) > CA_MAXDGNLEN ||
	    strlen (model) > CA_MAXMODELLEN ||
	    strlen (library) > CA_MAXTAPELIBLEN) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_ENTDGNMAP);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, dgn);
	marshall_STRING (sbp, model);
	marshall_STRING (sbp, library);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
