/*
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.10 $ $Date: 2007/03/23 13:08:33 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "net.h"
int sendrep(int rpfd, int rep_type, ...)
{
	va_list args;
	char func[16];
	char *msg;
	int n;
	char prtbuf[PRTBUFSZ];
	char *rbp;
	int rc;
	char repbuf[REPBUFSZ];
	int repsize;

	strcpy (func, "sendrep");
	rbp = repbuf;
	marshall_LONG (rbp, TPMAGIC);
	va_start (args, rep_type);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (args, char *);
		vsprintf (prtbuf, msg, args);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
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
		tplogit (func, "rc = %d\n", rc);
		break;
	}
	va_end (args);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
		tplogit (func, TP002, "send", neterror());
		if (rep_type == TAPERC)
			netclose (rpfd);
		return (-1);
	}
	if (rep_type == TAPERC)
		netclose (rpfd);
	return (0);
}
