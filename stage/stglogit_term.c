/*
 * $Id: stglogit_term.c,v 1.4 2001/01/31 19:00:12 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit_term.c,v $ $Revision: 1.4 $ $Date: 2001/01/31 19:00:12 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
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
