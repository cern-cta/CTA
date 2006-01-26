/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_getgrpbygid.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:18 $ CERN IT-GD/SC Jean-Philippe Baud";
#endif /* not lint */

/*      Cns_getgrpbygid - get group name associated with a given gid */

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
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_getgrpbygid(gid_t gid, char *groupname)
{
	int c;
	char func[16];
	int msglen;
	char *q;
	char *rbp;
	char repbuf[256];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;

	strcpy (func, "Cns_getgrpbygid");
	if (Cns_apiinit (&thip))
		return (-1);

	if (! groupname) {
		serrno = EFAULT;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_GETGRPNAM);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, gid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_STRING (rbp, groupname);
	}
	return (c);
}
