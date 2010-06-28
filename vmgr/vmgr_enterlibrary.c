/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      vmgr_enterlibrary - define a new tape library */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "vmgr_api.h"
#include "vmgr.h"
#include "serrno.h"
#include <string.h>

int vmgr_enterlibrary(const char *library_name, int capacity, int status)
{
	int c;
	char func[18];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct vmgr_api_thread_info *thip;
	uid_t uid;

        strcpy (func, "vmgr_enterlibrary");
        if (vmgr_apiinit (&thip))
                return (-1);
        uid = geteuid();
        gid = getegid();

	if (! library_name) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (library_name) > CA_MAXTAPELIBLEN) {
		serrno = EINVAL;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_ENTLIBRARY);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, library_name);
	marshall_LONG (sbp, capacity);
	marshall_LONG (sbp, status);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2vmgr (NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == EVMGRNACT)
		sleep (RETRYI);
	return (c);
}
