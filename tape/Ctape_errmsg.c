/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_errmsg.c,v $ $Revision: 1.3 $ $Date: 2000/05/03 06:14:13 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <varargs.h>
#include <sys/types.h>
#include "Ctape.h"

static char *errbufp = NULL;
static int errbuflen;

/*	Ctape_seterrbuf - set receiving buffer for error messages */

void
Ctape_seterrbuf(buffer, buflen)
char *buffer;
int buflen;
{
	errbufp = buffer;
	errbuflen = buflen;
}

/* Ctape_errmsg - send error message to user defined client buffer or to stderr */

Ctape_errmsg(va_alist) va_dcl
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
