/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_deltag.c,v $ $Revision: 1.1 $ $Date: 2003/10/28 11:13:25 $ CERN IT-GD/CT Jean-Philippe Baud";
#endif /* not lint */

/*	vmgr_deltag - delete a tag associated with a tape volume */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "vmgr_api.h"
#include "vmgr.h"

int DLL_DECL
vmgr_deltag(const char *vid)
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

	strcpy (func, "vmgr_deltag");
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

	if (strlen (vid) > CA_MAXVIDLEN) {
		serrno = EINVAL;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_DELTAG);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, vid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
