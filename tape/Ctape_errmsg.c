/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: Ctape_errmsg.c,v $ $Revision: 1.6 $ $Date: 2007/02/15 17:37:05 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
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

int Ctape_errmsg(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[PRTBUFSZ];
	int save_errno;

	save_errno = errno;
        va_start (args, msg);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);
	va_end (args);
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
