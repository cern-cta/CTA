/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <stdarg.h>
#include "rmc.h"
#include <Ctape_api.h>

int usrmsg(char *func, char *msg, ...)
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
