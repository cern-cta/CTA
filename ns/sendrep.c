/*
 * Copyright (C) 1993-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:23 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <stdarg.h>
#include "marshall.h"
#include "net.h"
#include "Cns.h"
sendrep(int rpfd, int rep_type, ...)
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
	char repbuf[REPBUFSZ+12];
	int repsize;

	strcpy (func, "sendrep");
	rbp = repbuf;
	marshall_LONG (rbp, CNS_MAGIC);
	va_start (args, rep_type);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (args, char *);
		vsprintf (prtbuf, msg, args);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		nslogit (func, "%s", prtbuf);
		break;
	case MSG_DATA:
	case MSG_LINKS:
	case MSG_REPLIC:
	case MSG_REPLICP:
		n = va_arg (args, int);
		marshall_LONG (rbp, n);
		msg = va_arg (args, char *);
		memcpy (rbp, msg, n);	/* marshalling already done */
		rbp += n;
		break;
	case CNS_IRC:
	case CNS_RC:
		rc = va_arg (args, int);
		marshall_LONG (rbp, rc);
		break;
	}
	va_end (args);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
		nslogit (func, NS002, "send", neterror());
		if (rep_type == CNS_RC)
			netclose (rpfd);
		return (-1);
	}
	if (rep_type == CNS_RC)
		netclose (rpfd);
	return (0);
}
