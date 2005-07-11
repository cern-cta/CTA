/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: usrmsg.c,v $ $Revision: 1.3 $ $Date: 2005/07/11 11:38:12 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <stdarg.h>
#include "rmc.h"

usrmsg(char *func, char *msg, ...)
{
	va_list args;
	char *p;
	char prtbuf[PRTBUFSZ];
	extern int rpfd;
	int save_errno;

	save_errno = errno;
	va_start (args, msg);
	sprintf (prtbuf, "%s: ", func);
	p = prtbuf + strlen (prtbuf);
	vsprintf (p, msg, args);
	sendrep (rpfd, MSG_ERR, "%s", prtbuf);
	va_end (args);
	errno = save_errno;
	return (0);
}
