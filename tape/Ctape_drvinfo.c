/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: Ctape_drvinfo.c,v $ $Revision: 1.2 $ $Date: 2007/02/15 17:37:05 $ CERN IT-GD/CT Jean-Philippe Baud"; */
#endif /* not lint */

/*	Ctape_drvinfo - get tape drive information */

#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "serrno.h"

int Ctape_drvinfo(drive, devinfo)
char *drive;
struct devinfo *devinfo;
{
	int c;
	char func[16];
	gid_t gid;
	int i;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[54];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Ctape_drvinfo");
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Ctape_errmsg (func, TP053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	if (drive == NULL || devinfo == NULL) {
		serrno = EFAULT;
		return (-1);
	}
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, DRVINFO);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, drive);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0) {
		rbp = repbuf;
		unmarshall_STRING (rbp, devinfo->devtype);
		unmarshall_WORD (rbp, devinfo->bsr);
		unmarshall_WORD (rbp, devinfo->eoitpmrks);
		unmarshall_WORD (rbp, devinfo->fastpos);
		unmarshall_WORD (rbp, devinfo->lddtype);
		unmarshall_LONG (rbp, devinfo->minblksize);
		unmarshall_LONG (rbp, devinfo->maxblksize);
		unmarshall_LONG (rbp, devinfo->defblksize);
		unmarshall_BYTE (rbp, devinfo->comppage);
		for (i = 0; i < CA_MAXDENFIELDS; i++) {
			unmarshall_WORD (rbp, devinfo->dencodes[i].den);
			unmarshall_BYTE (rbp, devinfo->dencodes[i].code);
		}
	}
	return (c);
}
