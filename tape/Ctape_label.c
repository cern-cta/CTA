/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_label.c,v $ $Revision: 1.9 $ $Date: 1999/12/19 09:48:26 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_label - send a request to the tape daemon to have a tape mounted
 *	and the label written
 */
#include <errno.h>
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
extern char *sys_errlist[];

Ctape_label(path, vid, partition, dgn, density, drive, vsn, lbltype, nbhdr, vdqm_reqid)
char *path;
char *vid;
int partition;
char *dgn;
char *density;
char *drive;
char *vsn;
char *lbltype;
int nbhdr;
int vdqm_reqid;
{
	char acctname[7];
	char actual_den[6];
	char actual_dgn[CA_MAXDGNLEN+1];
	char actual_lbltype[4];
	char actual_vid[CA_MAXVIDLEN+1];
	char actual_vsn[CA_MAXVIDLEN+1];
	int c, i, n;
	int den;
	char devtype[CA_MAXDVTLEN+1];
	int errflg = 0;
	char func[16];
	char *getacct();
	char *getcwd();
	gid_t gid;
	int jid;
	int lblcode;
	int mode = WRITE_ENABLE;
	int msglen;
	char *p, *q;
	char internal_path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[16];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *tempnam();
	uid_t uid;

	strcpy (func, "Ctape_label");
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Ctape_errmsg (func, TP053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif
	jid = getpid();
#ifdef TMS
	p = getacct();
	if (p == NULL) {
		Ctape_errmsg (func, TP027);
		serrno = SENOMAPFND;
		return (-1);
	}
	strcpy (acctname, p);
#else
	strcpy (acctname, "NOACCT");
#endif

	/* path */

	if (! path)
		strcpy (internal_path, tempnam (NULL, "tp"));

	/* vid */

	if (vid == NULL || *vid == '\0') {
		Ctape_errmsg (func, TP031);
		errflg++;
	} else {
		if (strlen (vid) > CA_MAXVIDLEN) {
			Ctape_errmsg (func, TP006, "vid");
			errflg++;
		} else {
			strcpy (actual_vid, vid);
			UPPER (actual_vid);
		}
	}

	if (errflg)
		return (EINVAL);

	/* Set default values */

#if TMS
	if (! vsn)
		actual_vsn[0] = '\0';
	else
		strcpy (actual_vsn, vsn);
	if (! dgn)
		actual_dgn[0] = '\0';
	else
		strcpy (actual_dgn, dgn);
	if (! density)
		actual_den[0] = '\0';
	else
		strcpy (actual_den, density);
	if (! lbltype)
		actual_lbltype[0] = '\0';
	else
		strcpy (actual_lbltype, lbltype);

	errflg += tmscheck (actual_vid, actual_vsn, actual_dgn, actual_den,
		actual_lbltype, mode, acctname);
#else
	if (! vsn)
		strcpy (actual_vsn, actual_vid);
	if (! dgn)
		strcpy (actual_dgn, DEFDGN);
	if (! density)
		strcpy (actual_den, "0");
	if (! lbltype)
		strcpy (actual_lbltype, "sl");
#endif
	if (errflg)
		return (EINVAL);
 
        /* Build request header */
 
        sbp = sendbuf;
        marshall_LONG (sbp, TPMAGIC);
        marshall_LONG (sbp, TPMOUNT);
        q = sbp;        /* save pointer. The next field will be updated */
        msglen = 3 * LONGSIZE;
        marshall_LONG (sbp, msglen);
 
        /* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_STRING (sbp, acctname);
	marshall_STRING (sbp, path ? path : internal_path);
	marshall_STRING (sbp, vid);
	marshall_WORD (sbp, partition);
	marshall_STRING (sbp, actual_dgn);
	marshall_STRING (sbp, actual_den);
	if (drive) {
		marshall_STRING (sbp, drive);
	} else {
		marshall_STRING (sbp, "");
	}
	marshall_WORD (sbp, mode);
	marshall_STRING (sbp, actual_vsn);
	marshall_STRING (sbp, actual_lbltype);
	marshall_WORD (sbp, nbhdr);
	marshall_LONG (sbp, vdqm_reqid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	return (c);
}
