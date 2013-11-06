/*
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "net.h"
#include <unistd.h>

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

	strncpy (func, "sendrep", 16);
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
			close (rpfd);
		return (-1);
	}
	if (rep_type == TAPERC)
		close (rpfd);
	return (0);
}
