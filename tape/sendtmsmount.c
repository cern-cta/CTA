/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendtmsmount.c,v $ $Revision: 1.6 $ $Date: 2000/06/30 15:01:01 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*	sendtmsmount -  send a MOUNT request to TMS */
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "serrno.h"
#if TMS
extern int rpfd;
sendtmsmount(mode, mtype, vid, jid, logonid, acctname, drive)
int mode;
char *mtype;
char *vid;
int jid;
char *logonid;
char *acctname;
char *drive;
{
	char account[3];
	int c;
	char func[16];
	int rc;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	char tmsreq[85];
	char userid[4];

	ENTRY (sendtmsmount);
	strncpy (userid, acctname, 3);
	userid[3] = '\0';
	strncpy (account, acctname + 4, 2);
	account[2] = '\0';
	sprintf (tmsreq,
		"MOUNT %c %s V %s US %s UNIQUE I%d LOGON %s ACC %s",
		(mode == WRITE_ENABLE) ? 'W' : 'R', mtype, vid, userid, jid,
		logonid, account);
	if (*drive)
		sprintf (tmsreq + strlen (tmsreq), " UN %s", drive);
	tplogit (func, "%s\n", tmsreq);
	reqlen = strlen (tmsreq);
	while (1) {
		repsize = sizeof(tmrepbuf);
		c = _sysreq ("root", acctname, "TMS", tmsreq, reqlen, tmrepbuf,
		    &repsize, 0);
		switch (c) {
		case 0:
		case 191:
		case 192:
			rc = 0;		/* ok */
			break;
		case 12:
		case 182:
		case 184:
		case 185:
		case 186:
		case 187:
		case 189:
		case 2193:
			sendrep (rpfd, MSG_ERR, "%s\n", tmrepbuf);
			rc = EACCES;	/* access denied or volume inactive */
			break;
		case 190:
		case 2190:
			sendrep (rpfd, MSG_ERR, "%s\n", tmrepbuf);
			rc = ETVBSY;	/* volume busy */
			break;
		case 188:
			sendrep (rpfd, MSG_ERR, "%s\n", tmrepbuf);
			rc = EACCES;	/* volume unknown */
			break;
		default:
			sleep (60);
			continue;
		}
		break;
	}
	RETURN (rc);
}
#endif
