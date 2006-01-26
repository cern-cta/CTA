/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_rmusrmap.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:20 $ CERN IT-GD/SC Jean-Philippe Baud";
#endif /* not lint */

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

/*      Cns_rmusrmap - suppress user entry corresponding to a given uid/name */

int DLL_DECL
Cns_rmusrmap(uid_t uid, char *username)
{
	int c;
	char func[16];
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;

	strcpy (func, "Cns_rmgrpmap");
	if (Cns_apiinit (&thip))
		return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_RMUSRMAP);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	if (username) {
		marshall_STRING (sbp, username);
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	return (c);
}
