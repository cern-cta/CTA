/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: usrmsg.c,v $ $Revision: 1.2 $ $Date: 1999/09/17 09:48:33 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#ifdef NOTRACE
#include <stdio.h>
#endif
#include <sys/types.h>
#include <varargs.h>
#include "Ctape.h"

usrmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
#ifdef NOTRACE
	char prtbuf[PRTBUFSZ];
#else
	char *p;
	extern struct tpdrep rep;
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
	sprintf (rep.data, "%s: ", func);
	p = rep.data + strlen (rep.data);
	vsprintf (p, msg, args);
	tplogit (func, "%s", p);
#endif
	va_end (args);
	errno = save_errno;
}
