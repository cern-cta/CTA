/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cupv_errmsg.c,v $ $Revision: 1.1 $ $Date: 2002/05/28 09:37:57 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <varargs.h>
#include <sys/types.h>
#include "Cupv.h"
#include "Cupv_api.h"

/*	Cupv_seterrbuf - set receiving buffer for error messages */

Cupv_seterrbuf(char *buffer, int buflen)
{
	struct Cupv_api_thread_info *thip;

	if (Cupv_apiinit (&thip))
		return (-1);
	thip->errbufp = buffer;
	thip->errbuflen = buflen;
	return (0);
}

/* Cupv_errmsg - send error message to user defined client buffer or to stderr */

Cupv_errmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	struct Cupv_api_thread_info *thip;

	save_errno = errno;
	if (Cupv_apiinit (&thip))
		return (-1);
        va_start (args);
        func = va_arg (args, char *);
        msg = va_arg (args, char *);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);
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














