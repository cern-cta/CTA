/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_errmsg.c,v $ $Revision: 1.2 $ $Date: 2000/05/03 05:37:28 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <varargs.h>
#include <sys/types.h>
#include "vmgr.h"
#include "vmgr_api.h"

/*	vmgr_seterrbuf - set receiving buffer for error messages */

vmgr_seterrbuf(char *buffer, int buflen)
{
	struct vmgr_api_thread_info *thip;

	if (vmgr_apiinit (&thip))
		return (-1);
	thip->errbufp = buffer;
	thip->errbuflen = buflen;
	return (0);
}

/* vmgr_errmsg - send error message to user defined client buffer or to stderr */

vmgr_errmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	struct vmgr_api_thread_info *thip;

	save_errno = errno;
	if (vmgr_apiinit (&thip))
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
