/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_position.c,v $ $Revision: 1.19 $ $Date: 2005/01/20 16:29:58 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_position - send a request to the tape daemon to get the tape
 *	positionned to a given file and the HDR labels checked
 */
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

Ctape_position(path, method, fseq, fsec, blockid, Qfirst, Qlast, filstat, fid, fsid, recfm, blksize, lrecl, retentd, flags)
char *path;
int method;
int fseq;
int fsec;
unsigned char *blockid;
int Qfirst;
int Qlast;
int filstat;
char *fid;
char *fsid;
char *recfm;
int blksize;
int lrecl;
int retentd;
int flags;
{
	char actual_fid[CA_MAXFIDLEN+1];
	int actual_fseq;
	char actual_fsid[CA_MAXVSNLEN+1];
	int c;
	int errflg = 0;
	char func[16];
	char fullpath[CA_MAXPATHLEN+1];
	char *getcwd();
	gid_t gid;
	char hdr1[81], hdr2[81];
	int jid;
	int msglen;
	int n;
	char *p;
	char *q;
	char *rbp;
	char repbuf[328];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char uhl1[81];
	uid_t uid;
	char vol1[81];

	strcpy (func, "Ctape_position");
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Ctape_errmsg (func, TP053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif
	jid = findpgrp();

	/* path */

	if (path == NULL || *path == '\0') {
		Ctape_errmsg (func, TP029);
		errflg++;
	} else {
		*fullpath = '\0';
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
        marshall_LONG (sbp, TPPOS);
        q = sbp;        /* save pointer. The next field will be updated */
        msglen = 3 * LONGSIZE;
        marshall_LONG (sbp, msglen);
 
        /* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_STRING (sbp, fullpath);
	marshall_WORD (sbp, method);
	if (method == TPPOSIT_FSEQ || method == TPPOSIT_BLKID)
		marshall_LONG (sbp, fseq);
	if (method == TPPOSIT_FSEQ || method == TPPOSIT_FID)
		marshall_WORD (sbp, fsec);
	if (method == TPPOSIT_BLKID)
		marshall_OPAQUE (sbp, blockid, 4);
	marshall_LONG (sbp, Qfirst);
	marshall_LONG (sbp, Qlast);
	marshall_WORD (sbp, filstat);
	if (fid && *fid) {
		p = fid;
		if ((n = strlen (fid) - 17) > 0) p += n;
		strcpy (actual_fid, p);
		UPPER (actual_fid);
		marshall_STRING (sbp, actual_fid);
	} else {
		marshall_STRING (sbp, "");
	}
	if (fsid) {
		strcpy (actual_fsid, fsid);
		UPPER (actual_fsid);
		marshall_STRING (sbp, actual_fsid);
	} else {
		marshall_STRING (sbp, "");
	}
	if (recfm) {
		marshall_STRING (sbp, recfm);
	} else {
		marshall_STRING (sbp, "");
	}
	marshall_LONG (sbp, blksize);
	marshall_LONG (sbp, lrecl);
	marshall_WORD (sbp, retentd);
	marshall_WORD (sbp, flags);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0) {
		rbp = repbuf;
		unmarshall_LONG (rbp, actual_fseq);
		unmarshall_STRING (rbp, vol1);
		unmarshall_STRING (rbp, hdr1);
		unmarshall_STRING (rbp, hdr2);
		unmarshall_STRING (rbp, uhl1);
		setlabelinfo (path, flags, actual_fseq, vol1, hdr1, hdr2, uhl1);
	}
	return (c);
}
