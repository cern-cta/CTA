/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_querytape.c,v $ $Revision: 1.7 $ $Date: 2001/01/19 11:20:41 $ CERN IT-PDP/DM Jean-Philippe Baud";
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

vmgr_querytape(const char *vid, char *vsn, char *dgn, char *density, char *lbltype, char *model, char *media_letter, char *manufacturer, char *sn, char *poolname, int *free_space, int *nbfiles, int *rcount, int *wcount, time_t *rtime, time_t *wtime, int *status)
{
	int c;
	char func[15];
	gid_t gid;
	int msglen;
	int n;
	char *q;
	char *rbp;
	char repbuf[126];
	char *sbp;
	char sendbuf[REQBUFSZ];
	time_t t1;
	struct vmgr_api_thread_info *thip;
	char tmpbuf[25];
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

	while ((c = send2vmgr (NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
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
		unmarshall_STRING (rbp, tmpbuf);
		if (model)
			strcpy (model, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (media_letter)
			strcpy (media_letter, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (manufacturer)
			strcpy (manufacturer, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (sn)
			strcpy (sn, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (poolname)
			strcpy (poolname, tmpbuf);
		unmarshall_LONG (rbp, n);
		if (free_space)
			*free_space = n;
		unmarshall_LONG (rbp, n);
		if (nbfiles)
			*nbfiles = n;
		unmarshall_LONG (rbp, n);
		if (rcount)
			*rcount = n;
		unmarshall_LONG (rbp, n);
		if (wcount)
			*wcount = n;
		unmarshall_TIME_T (rbp, t1);
		if (rtime)
			*rtime = t1;
		unmarshall_TIME_T (rbp, t1);
		if (wtime)
			*wtime = t1;
		unmarshall_LONG (rbp, n);
		if (status)
			*status = n;
	}
	return (c);
}
