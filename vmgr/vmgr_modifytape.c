/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_modifytape.c,v $ $Revision: 1.2 $ $Date: 1999/12/17 10:52:35 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*      vmgr_modifytape - modify an existing tape volume */

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

vmgr_modifytape(const char *vid, char *vsn, char *dgn, char *density, char *lbltype, char *model, char *media_letter, char *manufacturer, char *sn, char *poolname)
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

        strcpy (func, "vmgr_modifytape");
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
	marshall_LONG (sbp, VMGR_MODTAPE);
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
	if (dgn) {
		marshall_STRING (sbp, dgn);
	} else {
		marshall_STRING (sbp, "");
	}
	if (density) {
		marshall_STRING (sbp, density);
	} else {
		marshall_STRING (sbp, "");
	}
	if (lbltype) {
		marshall_STRING (sbp, lbltype);
	} else {
		marshall_STRING (sbp, "al");
	}
	if (model) {
		marshall_STRING (sbp, model);
	} else {
		marshall_STRING (sbp, "");
	}
	if (media_letter) {
		marshall_STRING (sbp, media_letter);
	} else {
		marshall_STRING (sbp, "");
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
	if (poolname) {
		marshall_STRING (sbp, poolname);
	} else {
		marshall_STRING (sbp, "");
	}
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2vmgr (NULL, sendbuf, msglen, NULL, 0);
	return (c);
}
