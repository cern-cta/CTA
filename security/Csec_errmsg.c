/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_errmsg.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>

#include "Csec.h"

/*	Csec_seterrbuf - set receiving buffer for error messages */

int DLL_DECL
Csec_seterrbuf(char *buffer, int buflen)
{
	struct Csec_api_thread_info *thip;

	if (Csec_apiinit (&thip))
		return (-1);
	thip->errbufp = buffer;
	thip->errbuflen = buflen;
	return (0);
}

/* Csec_errmsg - send error message to user defined client buffer or to stderr */
int DLL_DECL
Csec_errmsg(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	struct Csec_api_thread_info *thip;
    int funlen=0;
    
    save_errno = errno;

    if (Csec_apiinit (&thip))
		return (-1);
    va_start (args, msg);
    
    if (func)
		snprintf (prtbuf, PRTBUFSZ, "%s: ", func);
	else
		*prtbuf = '\0';

    funlen = strlen(prtbuf);

    vsnprintf (prtbuf + funlen, PRTBUFSZ - funlen -1, msg, args);
    prtbuf[PRTBUFSZ] = '\0';
    
	if (thip->errbufp) {
		if (strlen (prtbuf) < thip->errbuflen) {
			strcpy (thip->errbufp, prtbuf);
		} else {
			strncpy (thip->errbufp, prtbuf, thip->errbuflen - 2);
			thip->errbufp[thip->errbuflen-2] = '\n';
			thip->errbufp[thip->errbuflen-1] = '\0';
		}
	} else {
		fprintf (stderr, "%s\n", prtbuf);
	}

    Csec_trace("ERROR", "%s\n", prtbuf);
    
    errno = save_errno;
	return (0);
}
