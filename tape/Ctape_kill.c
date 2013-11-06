/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_kill - cancel a tape mount or position request */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "h/Ctape.h"
#include "h/marshall.h"
#include "h/serrno.h"
#include "h/Ctape_api.h"

int Ctape_kill(char *path)
{
	int c;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	char func[16];
	char *getcwd();
	gid_t gid;
	int jid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strncpy (func, "Ctape_kill", 16);
	uid = getuid();
	gid = getgid();
	jid = findpgrp();

	/* path */

	if (path == NULL || *path == '\0') {
		Ctape_errmsg (func, TP029);
		errflg++;
	} else {
		fullpath[0] = '\0';
		if (*path != '/') {
			if (getcwd (fullpath, sizeof(fullpath) - 2) == NULL) {
				Ctape_errmsg (func, TP002, "getcwd", strerror(errno));
				errflg++;
			} else {
				strcat (fullpath, "/");
			}
		}
		if (strlen(fullpath) + strlen(path) < sizeof(fullpath)) {
			strcat (fullpath, path);
		} else {
			Ctape_errmsg (func, TP038);
			errflg++;
		}
	}

	if (errflg) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPKILL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_STRING (sbp, fullpath);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, NULL, 0);
	return (c);
}
