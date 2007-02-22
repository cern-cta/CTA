/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
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

int vmgr_modifytape(const char *vid, char *vsn, char *library, char *density, char *lbltype, char *manufacturer, char *sn, char *poolname, int status)
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
		marshall_STRING (sbp, "");
	}
	if (library) {
		marshall_STRING (sbp, library);
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
	marshall_LONG (sbp, status);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
