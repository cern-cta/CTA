/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: usrmsg.c,v $ $Revision: 1.6 $ $Date: 2000/05/04 10:12:07 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#ifdef NOTRACE
#include <stdio.h>
#endif
#include <string.h>
#include <sys/types.h>
#include <varargs.h>
#include "Ctape.h"

usrmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
#ifndef NOTRACE
	char *p;
	extern int rpfd;
#endif
	int save_errno;

	save_errno = errno;
	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
#ifdef NOTRACE
	vsprintf (prtbuf, msg, args);
	Ctape_errmsg (func, "%s\n", prtbuf);
#else
	sprintf (prtbuf, "%s: ", func);
	p = prtbuf + strlen (prtbuf);
	vsprintf (p, msg, args);
	tplogit (func, "%s", p);
	if (rpfd >= 0)
		sendrep (rpfd, MSG_ERR, "%s", prtbuf);
#endif
	va_end (args);
	errno = save_errno;
	return (0);
}
