/*
 * $Id: stglogit_term.c,v 1.7 2001/03/09 10:58:27 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit_term.c,v $ $Revision: 1.7 $ $Date: 2001/03/09 10:58:27 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#if (defined(__osf__) && defined(__alpha))
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#else
	snprintf (prtbuf, PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#endif
	prtbuf[PRTBUFSZ-1] = '\0';
	if ((strlen(prtbuf) + 1) < PRTBUFSZ) {
#if (defined(__osf__) && defined(__alpha))
		vsprintf (prtbuf+strlen(prtbuf), msg, args);
#else
		vsnprintf (prtbuf+strlen(prtbuf), PRTBUFSZ - strlen(prtbuf), msg, args);
#endif
    }
	prtbuf[PRTBUFSZ-1] = '\0';
	va_end (args);
	fprintf(stdout, prtbuf, strlen(prtbuf));
	return(0);
}
