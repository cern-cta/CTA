/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: usrmsg.c,v $ $Revision: 1.8 $ $Date: 2007/02/22 17:26:25 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#ifdef NOTRACE
#include <stdio.h>
#endif
#include <string.h>
#include <sys/types.h>
#include <stdarg.h>
#include "Ctape.h"
#include "Ctape_api.h"

int usrmsg(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[PRTBUFSZ];
#ifndef NOTRACE
	char *p;
	extern int rpfd;
#endif
	int save_errno;

	save_errno = errno;
	va_start (args, msg);
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
