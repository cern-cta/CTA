/*
 * $Id: stglogit_term.c,v 1.10 2003/11/04 13:28:12 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit_term.c,v $ $Revision: 1.10 $ $Date: 2003/11/04 13:28:12 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include "stage_constants.h"
#include "Csnprintf.h"

extern int reqid;

int stglogit(char *func, char *msg, ...) {
	va_list args;
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;

	va_start (args, msg);
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
	Csnprintf (prtbuf, PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
	prtbuf[PRTBUFSZ-1] = '\0';
	if ((strlen(prtbuf) + 1) < PRTBUFSZ) {
		Cvsnprintf (prtbuf+strlen(prtbuf), PRTBUFSZ - strlen(prtbuf), msg, args);
    }
	prtbuf[PRTBUFSZ-1] = '\0';
	va_end (args);
	fprintf(stdout, prtbuf, strlen(prtbuf));
	return(0);
}
