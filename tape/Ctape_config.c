/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#) $Id: Ctape_config.c,v 1.4 1999/09/03 15:53:36 baud Exp $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_config - configure a drive up/down */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Ctape.h"
#include "marshall.h"
#include "serrno.h"

Ctape_config(unm, status, reason)
char *unm;
int status;
int reason;
{
	int c, n;
	char drive[CA_MAXUNMLEN+1];
	char func[16];
	gid_t gid;
	char *host;
	int msglen;
	char *p, *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Ctape_config");
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, TP053);
		return (USERR);
	}
#endif

	/* unm may be in the form drivename@hostname */

	if (p = strchr (unm, '@')) {
		if ((p - unm) > CA_MAXUNMLEN)
			return (ETPRM);
		strncpy (drive, unm, p - unm);
		drive[p-unm] = '\0';
		host = p + 1;
	} else
		host = NULL;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPCONF);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_WORD (sbp, uid);
	marshall_WORD (sbp, gid);
	marshall_STRING (sbp, drive);
	marshall_WORD (sbp, status);
	marshall_WORD (sbp, reason);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (host, sendbuf, msglen, NULL, 0);
	return (c);
}
