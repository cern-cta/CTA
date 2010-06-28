/*
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <netinet/in.h>
#include <stdarg.h>
#include "marshall.h"
#include "net.h"
#include "vmgr_server.h"
#include "vmgr.h"

int sendrep(int rpfd, int rep_type, ...)
{
	va_list args;
	char func[16];
	char *msg;
	int n;
	char prtbuf[PRTBUFSZ];
	char *rbp;
	int rc;
	char repbuf[REPBUFSZ+12];
	int repsize;

	strcpy (func, "sendrep");
	rbp = repbuf;
	marshall_LONG (rbp, VMGR_MAGIC);
	va_start (args, rep_type);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (args, char *);
		vsprintf (prtbuf, msg, args);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		vmgrlogit (func, "%s", prtbuf);
		break;
	case MSG_DATA:
		n = va_arg (args, int);
		marshall_LONG (rbp, n);
		msg = va_arg (args, char *);
		memcpy (rbp, msg, n);	/* marshalling already done */
		rbp += n;
		break;
	case VMGR_IRC:
	case VMGR_RC:
		rc = va_arg (args, int);
		marshall_LONG (rbp, rc);
		break;
	}
	va_end (args);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
		vmgrlogit (func, VMG02, "send", neterror());
		if (rep_type == VMGR_RC)
			netclose (rpfd);
		return (-1);
	}
	if (rep_type == VMGR_RC)
		netclose (rpfd);
	return (0);
}
