/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rmc_errmsg.c,v $ $Revision: 1.1 $ $Date: 2002/11/29 08:51:47 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <varargs.h>
#include <sys/types.h>
#include "rmc.h"

static char *errbufp = NULL;
static int errbuflen;

/*	rmc_seterrbuf - set receiving buffer for error messages */

void
rmc_seterrbuf(buffer, buflen)
char *buffer;
int buflen;
{
	errbufp = buffer;
	errbuflen = buflen;
}

/* rmc_errmsg - send error message to user defined client buffer or to stderr */

rmc_errmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	int save_errno;

	save_errno = errno;
        va_start (args);
        func = va_arg (args, char *);
        msg = va_arg (args, char *);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);
	if (errbufp) {
		if (strlen (prtbuf) < errbuflen) {
			strcpy (errbufp, prtbuf);
		} else {
			strncpy (errbufp, prtbuf, errbuflen - 2);
			errbufp[errbuflen-2] = '\n';
			errbufp[errbuflen-1] = '\0';
		}
	} else {
		fprintf (stderr, "%s", prtbuf);
	}
	errno = save_errno;
	return (0);
}
