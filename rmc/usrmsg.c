/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: usrmsg.c,v $ $Revision: 1.1 $ $Date: 2002/11/29 08:51:49 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <varargs.h>
#include "rmc.h"

usrmsg(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char *p;
	char prtbuf[PRTBUFSZ];
	extern int rpfd;
	int save_errno;

	save_errno = errno;
	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
	sprintf (prtbuf, "%s: ", func);
	p = prtbuf + strlen (prtbuf);
	vsprintf (p, msg, args);
	sendrep (rpfd, MSG_ERR, "%s", prtbuf);
	va_end (args);
	errno = save_errno;
	return (0);
}
