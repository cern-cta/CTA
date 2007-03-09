/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
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

int rmc_errmsg(char *func, char *msg, ...)
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
	va_end (args);
	errno = save_errno;
	return (0);
}
