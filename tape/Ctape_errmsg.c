/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */


#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include "h/Ctape.h"

static char *errbufp = NULL;
static int errbuflen;

/*	Ctape_seterrbuf - set receiving buffer for error messages */

void
Ctape_seterrbuf(char *buffer,
                     int buflen)
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
		if ((int)strlen (prtbuf) < errbuflen) {
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
