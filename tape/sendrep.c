/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.1 $ $Date: 1999/09/20 06:19:06 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <sys/types.h>
#include <string.h>
#include <varargs.h>
#include <netinet/in.h>
#include "Ctape.h"
#include "marshall.h"
#include "net.h"
extern char *sys_errlist[];
sendrep(va_alist) va_dcl
{
	va_list args;
	char func[16];
	char *msg;
	int n;
	char *p;
	char prtbuf[PRTBUFSZ];
	char *q;
	char *rbp;
	int rc;
	int rep_type;
	int req_type;
	char repbuf[REPBUFSZ];
	int repsize;
	int rpfd;

	ENTRY (sendrep);
	rbp = repbuf;
	marshall_LONG (rbp, TPMAGIC);
	va_start (args);
	rpfd = va_arg (args, int);
	rep_type = va_arg (args, int);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (args, char *);
		vsprintf (prtbuf, msg, args);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		tplogit (func, "%s", prtbuf);
		break;
	case MSG_DATA:
		n = va_arg (args, int);
		marshall_LONG (rbp, n);
		msg = va_arg (args, char *);
		memcpy (rbp, msg, n);	/* marshalling already done */
		rbp += n;
		break;
	case TAPERC:
		rc = va_arg (args, int);
		marshall_LONG (rbp, rc);
		break;
	}
	va_end (args);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
		tplogit (func, TP002, "write", sys_errlist[errno]);
		if (rep_type == TAPERC)
			close (rpfd);
		RETURN (-1);
	}
	if (rep_type == TAPERC)
		close (rpfd);
	RETURN (0);
}
