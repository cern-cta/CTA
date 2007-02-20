/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: Ctape_label.c,v $ $Revision: 1.24 $ $Date: 2007/02/20 16:56:34 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	Ctape_label - send a request to the tape daemon to have a tape mounted
 *	and the label written
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
#include "Ctape_api.h"
#include "vmgr_api.h"

int Ctape_label(path, vid, side, dgn, density, drive, vsn, lbltype, nbhdr, flags, vdqm_reqid)
char *path;
char *vid;
int side;
char *dgn;
char *density;
char *drive;
char *vsn;
char *lbltype;
int nbhdr;
int flags;
int vdqm_reqid;
{
	char acctname[7];
	char actual_den[6];
	char actual_dgn[CA_MAXDGNLEN+1];
	char actual_lbltype[CA_MAXLBLTYPLEN+1];
	char actual_vid[CA_MAXVIDLEN+1];
	char actual_vsn[CA_MAXVSNLEN+1];
	int c;
	int errflg = 0;
	char func[16];
	char *getacct();
	char *getcwd();
	gid_t gid;
	int jid;
	int mode = WRITE_ENABLE;
	int msglen;
	char *q;
	char internal_path[CA_MAXPATHLEN+1];
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
	jid = findpgrp();
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

	if (errflg) {
		serrno = EINVAL;
		return (-1);
	}

	/* Set default values */

#if TMS || VMGR
	if (! vsn)
		actual_vsn[0] = '\0';
	else {
		strcpy (actual_vsn, vsn);
		UPPER (actual_vsn);
	}
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

#if VMGR
	if ((c = vmgrcheck (actual_vid, actual_vsn, actual_dgn, actual_den,
                           actual_lbltype, mode, uid, gid))) {
#if TMS
		if (c != ETVUNKN)
#endif
		{
			Ctape_errmsg ("vmgrcheck", "%s\n", sstrerror(c));
			serrno = c;
			return (-1);
		}
#endif
#if TMS
		if (c = tmscheck (actual_vid, actual_vsn, actual_dgn, actual_den,
		    actual_lbltype, mode, acctname)) {
			serrno = c;
			return (-1);
		}
#endif
#if VMGR
	}
#endif
#else
	if (! vsn)
		strcpy (actual_vsn, actual_vid);
	else {
		strcpy (actual_vsn, vsn);
		UPPER (actual_vsn);
	}
	if (! dgn)
		strcpy (actual_dgn, DEFDGN);
	else
		strcpy (actual_dgn, dgn);
	if (! density)
		strcpy (actual_den, "0");
	else
		strcpy (actual_den, density);
	if (! lbltype)
		strcpy (actual_lbltype, "sl");
	else
		strcpy (actual_lbltype, lbltype);
#endif
 
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
	marshall_STRING (sbp, actual_vid);
	marshall_WORD (sbp, side);
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
	if (flags == DOUBLETM)
		nbhdr |= DOUBLETM;
	marshall_WORD (sbp, nbhdr);
	marshall_LONG (sbp, vdqm_reqid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	return (c);
}
