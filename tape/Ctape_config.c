/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: Ctape_config.c,v $ $Revision: 1.13 $ $Date: 2007/02/15 17:00:44 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	Ctape_config - configure a drive up/down */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "Ctape.h"
#include "marshall.h"
#include "serrno.h"
#include "Ctape_api.h"

int Ctape_config(unm, status, reason)
char *unm;
int status;
int reason;
{
	int c;
	char drive[CA_MAXUNMLEN+1];
	char func[16];
	gid_t gid;
	char *host;
	int msglen;
	char *p, *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Ctape_config");
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Ctape_errmsg (func, TP053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* unm may be in the form drivename@hostname */

	if ((p = strchr (unm, '@'))) {
		if ((p - unm) > CA_MAXUNMLEN) {
			serrno = EINVAL;
			return (-1);
		}
		strncpy (drive, unm, p - unm);
		drive[p-unm] = '\0';
		host = p + 1;
	} else {
		strcpy (drive, unm);
		host = NULL;
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPCONF);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, drive);
	marshall_WORD (sbp, status);
	marshall_WORD (sbp, reason);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (host, sendbuf, msglen, repbuf, sizeof(repbuf));
	return (c);
}
