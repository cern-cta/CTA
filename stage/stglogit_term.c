/*
 * $Id: stglogit_term.c,v 1.1 2000/03/23 00:36:21 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit_term.c,v $ $Revision: 1.1 $ $Date: 2000/03/23 00:36:21 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <varargs.h>
#include "stage.h"
extern int reqid;

int stglogit(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;

#ifdef __INSURE__
		/* Insure do not like at all this routine... */
		return(0);
#endif

	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
	vsprintf (prtbuf+strlen(prtbuf), msg, args);
	va_end (args);
	fprintf(stdout, prtbuf, strlen(prtbuf));
		return(0);
}
