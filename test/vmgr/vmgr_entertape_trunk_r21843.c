/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_entertape - enter a new tape volume */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "serrno_trunk_r21843.h"

int vmgr_entertape(const char *vid, char *vsn, char *library, char *density, char *lbltype, char *model, char *media_letter, char *manufacturer, char *sn, int nbsides, char *poolname, int status)
{
	int c;
	char func[15];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strncpy (func, "vmgr_entertape", 15);
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();

	if (! vid || ! library || ! density || ! model) {
		serrno = EFAULT;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_ENTTAPE);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, vid);
	if (vsn) {
		marshall_STRING (sbp, vsn);
	} else {
		marshall_STRING (sbp, vid);
	}
	marshall_STRING (sbp, library);
	marshall_STRING (sbp, density);
	if (lbltype) {
		marshall_STRING (sbp, lbltype);
	} else {
		marshall_STRING (sbp, "al");
	}
	marshall_STRING (sbp, model);
	if (media_letter) {
		marshall_STRING (sbp, media_letter);
	} else {
		marshall_STRING (sbp, " ");
	}
	if (manufacturer) {
		marshall_STRING (sbp, manufacturer);
	} else {
		marshall_STRING (sbp, "");
	}
	if (sn) {
		marshall_STRING (sbp, sn);
	} else {
		marshall_STRING (sbp, "");
	}
	marshall_WORD (sbp, nbsides);
	if (poolname) {
		marshall_STRING (sbp, poolname);
	} else {
		marshall_STRING (sbp, "");
	}
	marshall_LONG (sbp, status);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
