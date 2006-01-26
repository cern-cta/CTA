/*
 * Copyright (C) 2004-2005 by CERN/IT/GD/CT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_starttrans.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:21 $ CERN IT-GD/CT Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_starttrans - start transaction mode */

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
Cns_starttrans(char *server, char *comment)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	int s = -1;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;
 
	strcpy (func, "Cns_starttrans");
	if (Cns_apiinit (&thip))
		return (-1);
	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, comment ? CNS_MAGIC2 : CNS_MAGIC);
	marshall_LONG (sbp, CNS_STARTTRANS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	if (comment) {
		marshall_STRING (sbp, comment);
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (&s, server, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0)
		thip->fd = s;
	return (c);
}
