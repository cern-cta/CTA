/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_querytape.c,v $ $Revision: 1.1 $ $Date: 1999/12/15 10:02:17 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*      vmgr_querytape - query about a tape volume */

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

vmgr_querytape(const char *vid, char *vsn, char *dgn, char *density, char *lbltype)
{
	int c;
	char func[15];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[26];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	char tmpbuf[10];
	uid_t uid;

        strcpy (func, "vmgr_querytape");
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
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_QRYTAPE);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, vid);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2vmgr (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0) {
		rbp = repbuf;
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
	}
	return (c);
}
