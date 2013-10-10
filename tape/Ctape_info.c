/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_info - get tape file information */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "h/Ctape.h"
#include "h/marshall.h"
#include "h/serrno.h"
#include "h/Ctape_api.h"

int Ctape_info(char *path,
               int *blksize,
               unsigned char *blockid,
               char *density,
               char *devtype,
               char *drive,
               char *fid,
               int *fseq,
               int *lrecl,
               char *recfm)
{
	int c, n;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	char func[16];
	char *getcwd();
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[66];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char tmpbuf[20];
	uid_t uid;
 
	strncpy (func, "Ctape_info", 16);
	uid = getuid();
	gid = getgid();

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
	marshall_LONG (sbp, TPINFO);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fullpath);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0) {
		rbp = repbuf;
		unmarshall_LONG (rbp, n);
		if (blksize)
			*blksize = n;
		unmarshall_OPAQUE (rbp, tmpbuf, 4);
		if (blockid)
			memcpy (blockid, tmpbuf, 4);
		unmarshall_STRING (rbp, tmpbuf);
		if (density)
			strcpy (density, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (devtype)
			strcpy (devtype, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (drive)
			strcpy (drive, tmpbuf);
		unmarshall_STRING (rbp, tmpbuf);
		if (fid)
			strcpy (fid, tmpbuf);
		unmarshall_LONG (rbp, n);
		if (fseq)
			*fseq = n;
		unmarshall_LONG (rbp, n);
		if (lrecl)
			*lrecl = n;
		unmarshall_STRING (rbp, tmpbuf);
		if (recfm)
			strcpy (recfm, tmpbuf);
	}
	return (c);
}
