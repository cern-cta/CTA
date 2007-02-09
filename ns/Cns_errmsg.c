/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Csnprintf.h"

/*	Cns_seterrbuf - set receiving buffer for error messages */

int DLL_DECL
Cns_seterrbuf(char *buffer, int buflen)
{
	struct Cns_api_thread_info *thip;

	if (Cns_apiinit (&thip))
		return (-1);
	thip->errbufp = buffer;
	thip->errbuflen = buflen;
	return (0);
}

/* Cns_errmsg - send error message to user defined client buffer or to stderr */

int DLL_DECL
Cns_errmsg(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	struct Cns_api_thread_info *thip;

	save_errno = errno;
	if (Cns_apiinit (&thip))
		return (-1);
	va_start (args, msg);
	if (func) {
		Csnprintf (prtbuf, PRTBUFSZ, "%s: ", func);
		prtbuf[PRTBUFSZ-1] = '\0';
	} else {
		*prtbuf = '\0';
	}
	if ((strlen(prtbuf) + 1) < PRTBUFSZ) {
		Cvsnprintf (prtbuf + strlen(prtbuf), PRTBUFSZ - strlen(prtbuf), msg, args);
		prtbuf[PRTBUFSZ-1] = '\0';
	}
	va_end (args);
	if (thip->errbufp) {
		if (strlen (prtbuf) < thip->errbuflen) {
			strcpy (thip->errbufp, prtbuf);
		} else {
			strncpy (thip->errbufp, prtbuf, thip->errbuflen - 2);
			thip->errbufp[thip->errbuflen-2] = '\n';
			thip->errbufp[thip->errbuflen-1] = '\0';
		}
	} else {
		fprintf (stderr, "%s", prtbuf);
	}
	errno = save_errno;
	return (0);
}
