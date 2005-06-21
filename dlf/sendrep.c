/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.3 $ $Date: 2005/06/21 11:21:54 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

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
#include "dlf.h"
#include "socket_timeout.h"

int sendrep(int rpfd, int rep_type, ...)
{
	va_list ap;
	char func[16];
	char *msg;
	int n;
	char *p;
	char prtbuf[DLF_PRTBUFSZ];
	char *q;
	char *rbp;
	int rc;
	int req_type;
	char repbuf[DLF_REPBUFSZ+12];
	int repsize;

	strcpy (func, "sendrep");
	rbp = repbuf;
	marshall_LONG (rbp, DLF_MAGIC);

	va_start (ap, rep_type);

	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (ap, char *);
		vsprintf (prtbuf, msg, ap);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		dlflogit (func, "%s", prtbuf);
		break;
	case MSG_DATA:
		n = va_arg (ap, int);
		marshall_LONG (rbp, n);
		msg = va_arg (ap, char *);
		memcpy (rbp, msg, n);	/* marshalling already done */
		rbp += n;
		break;
	case DLF_IRC:
	case DLF_RC:
		rc = va_arg (ap, int);
		marshall_LONG (rbp, rc);
		break;
	}
	va_end (ap);
	repsize = rbp - repbuf;
	if (netwrite_timeout (rpfd, repbuf, repsize, DLF_TIMEOUT) != repsize) {
	  	dlflogit (func, DLF02, "netwrite_timeout", neterror());
		if (rep_type == DLF_RC)
			netclose (rpfd);
		return (-1);
	}
	if (rep_type == DLF_RC)
		netclose (rpfd);
	return (0);
}
