/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_rls - unload tape and release reservations */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "Ctape.h"
#include "marshall.h"
#include "serrno.h"
#include "Ctape_api.h"

int Ctape_rls(path, flags)
char *path;
int flags;
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
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Ctape_rls");
	uid = getuid();
	gid = getgid();
	jid = findpgrp();

	/* path */

	if (path) {
		if (*path == '\0') {
			Ctape_errmsg (func, TP006, "path");
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
	}

	if (errflg) {
		serrno = EINVAL;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPRLS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, flags);
	if (path) {
		marshall_STRING (sbp, fullpath);
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0)
		(void) rmlabelinfo (path, flags);
	return (c);
}
