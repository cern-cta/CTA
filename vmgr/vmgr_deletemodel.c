/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_deletemodel.c,v $ $Revision: 1.2 $ $Date: 2000/01/03 09:59:16 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*      vmgr_deletemodel - delete a model of cartridge */

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

vmgr_deletemodel(const char *model, char *media_letter)
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

        strcpy (func, "vmgr_deletemodel");
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

	if (! model) {
		serrno = EFAULT;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_DELMODEL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, model);
	marshall_STRING (sbp, media_letter);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2vmgr (NULL, sendbuf, msglen, NULL, 0);
	return (c);
}
