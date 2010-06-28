/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_config - configure a drive up/down */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
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
