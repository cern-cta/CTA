/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_enterpool.c,v $ $Revision: 1.1 $ $Date: 2000/01/29 08:51:51 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*      vmgr_enterpool - define a new tape pool */

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

vmgr_enterpool(const char *pool_name, uid_t pool_user, gid_t pool_group)
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

        strcpy (func, "vmgr_enterpool");
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

	if (! pool_name) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (pool_name) > 24) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_ENTPOOL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, pool_name);
	marshall_LONG (sbp, pool_user);
	marshall_LONG (sbp, pool_group);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2vmgr (NULL, sendbuf, msglen, NULL, 0);
	return (c);
}
