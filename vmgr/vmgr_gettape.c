/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_gettape - get a tape volume to store a given amount of data */

#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"

int vmgr_gettape(const char *poolname, u_signed64 Size, const char *Condition, char *vid, char *vsn, char *dgn, char *density, char *lbltype, char *model, int *side, int *fseq, u_signed64 *estimated_free_space)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	int n;
	char *q;
	char *rbp;
	char repbuf[55];
	char *sbp;
	struct vmgr_api_thread_info *thip;
	char sendbuf[REQBUFSZ];
	char tmpbuf[9];
	u_signed64 u64;
	uid_t uid;

        strcpy (func, "vmgr_gettape");
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();
#if defined(_WIN32)
        if (uid < 0 || gid < 0) {
                vmgr_errmsg (func, VMG53);
                serrno = SENOMAPFND;
                return (-1);
        }
#endif

	if (! vid) {
		serrno = EFAULT;
		return (-1);
	}
	if ((poolname && strlen (poolname) > CA_MAXPOOLNAMELEN) || Size <= 0) {
		serrno = EINVAL;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC2);
	marshall_LONG (sbp, VMGR_GETTAPE);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	if (poolname) {
		marshall_STRING (sbp, poolname);
	} else {
		marshall_STRING (sbp, "");
	}
	marshall_HYPER (sbp, Size);
	if (Condition) {
		marshall_STRING (sbp, Condition);
	} else {
		marshall_STRING (sbp, "");
	}
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_STRING (rbp, vid);
		unmarshall_STRING (rbp, tmpbuf);
		if (vsn)
			strcpy (vsn, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (dgn)
			strcpy (dgn, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (density)
			strcpy (density, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (lbltype)
			strcpy (lbltype, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (model)
			strcpy (model, tmpbuf);
		unmarshall_WORD (rbp, n);
		if (side)
			*side = n;
		unmarshall_LONG (rbp, n);
		if (fseq)
			*fseq = n;
		unmarshall_HYPER (rbp, u64);
		if (estimated_free_space)
			*estimated_free_space = u64;
	}
	return (c);
}
