/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_tpmounted - update tape volume access time/count */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"

int vmgr_tpmounted(const char *vid, int mode, int jid)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	struct vmgr_api_thread_info *thip;
	char sendbuf[REQBUFSZ];
	uid_t uid;

        strncpy (func, "vmgr_tpmounted", 16);
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC2);
	marshall_LONG (sbp, VMGR_TPMOUNTED);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, vid);
	marshall_WORD (sbp, mode);
	marshall_LONG (sbp, jid);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
