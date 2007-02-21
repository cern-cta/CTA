/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tmscheck.c,v $ $Revision: 1.10 $ $Date: 2007/02/21 16:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */
 
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "serrno.h"
#if TMS
tmscheck(vid, vsn, dgn, den, lbl, mode, acctname)
char *vid;
char *vsn;
char *dgn;
char *den;
char *lbl;
int mode;
char *acctname;
{
	char account[3];
	int c, j;
	int errflg = 0;
	char func[16];
	char *p;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	char tmsden[6];
	char tmsdgn[7];
	char tmslbl[4];
	char tmsreq[80];
	char tmsvsn[7];
	char userid[4];

	strcpy (func, "tmscheck");
	if (mode == WRITE_DISABLE) {
		sprintf (tmsreq, "QVOL %s (GENERIC SHIFT MESSAGE", vid);
	} else {
		strncpy (userid, acctname, 3);
		userid[3] = '\0';
		strncpy (account, acctname + 4, 2);
		account[2] = '\0';
		sprintf (tmsreq, "QVOL %s WRITE %s %s (GENERIC SHIFT MESSAGE",
			vid, userid, account);
	}
	reqlen = strlen (tmsreq);
	while (1) {
		repsize = sizeof(tmrepbuf);
		c = sysreq ("TMS", tmsreq, &reqlen, tmrepbuf, &repsize);
		switch (c) {
		case 0:
			break;
		case 8:
			Ctape_errmsg (func, "%s\n", tmrepbuf);
			return (EINVAL);
		case 100:
			Ctape_errmsg (func, "%s\n", tmrepbuf);
			return (ETVUNKN);
		case 312:
		case 315:
			Ctape_errmsg (func, "%s\n", tmrepbuf);
			return (ETARCH);
		case 12:
			if (tmrepbuf[41] == 'R') {
				Ctape_errmsg (func, "Volume locked read-only\n");
				return (ETWPROT);
			} else {
				Ctape_errmsg (func, "%s\n", tmrepbuf);
				return (EACCES);
			}
		default:
			sleep (60);
			continue;
		}
		break;
	}
	strncpy (tmsvsn, tmrepbuf, 6);
	tmsvsn[6] = '\0';
	for  (j = 0; tmsvsn[j]; j++)
		if (tmsvsn[j] == ' ') break;
	tmsvsn[j] = '\0';
	if (*vsn) {
		if (strcmp (vsn, tmsvsn)) {
			Ctape_errmsg (func, TP055, vid, vsn, tmsvsn);
			errflg++;
		}
	} else {
		strcpy (vsn, tmsvsn);
	}

	strncpy (tmsdgn, tmrepbuf+ 25, 6);
	tmsdgn[6] = '\0';
	for  (j = 0; tmsdgn[j]; j++)
		if (tmsdgn[j] == ' ') break;
	tmsdgn[j] = '\0';
	if (strcmp (tmsdgn, "CT1") == 0) strcpy (tmsdgn, "CART");
	if (*dgn) {
		if (strcmp (dgn, tmsdgn) != 0) {
			Ctape_errmsg (func, TP055, vid, dgn, tmsdgn);
			errflg++;
		}
	} else {
		strcpy (dgn, tmsdgn);
	}

	p = tmrepbuf + 32;
	while (*p == ' ') p++;
	j = tmrepbuf + 40 - p;
	strncpy (tmsden, p, j);
	tmsden[j] = '\0';
	if (*den) {
		if (strcmp (den, tmsden) != 0) {
			Ctape_errmsg (func, TP055, vid, den, tmsden);
			errflg++;
		}
	} else {
		strcpy (den, tmsden);
	}

	tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
	tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
	tmslbl[2] = (tmrepbuf[76] == ' ') ? '\0' : tmrepbuf[76] - 'A' + 'a';
	tmslbl[3] = '\0';
	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tmslbl)) {
			Ctape_errmsg (func, TP055, vid, lbl, tmslbl);
			errflg++;
		}
	} else {
		strcpy (lbl, tmslbl);
	}
	return (errflg ? EINVAL : 0);
}
#endif
