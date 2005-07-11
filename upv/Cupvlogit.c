/*
 * Copyright (C) 1999-2002 by CERN IT-DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: Cupvlogit.c,v $ $Revision: 1.3 $ $Date: 2005/07/11 12:48:01 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "Cglobals.h"
#include "Cupv.h"
extern int jid;

Cupvlogit(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[LOGBUFSZ];
	int save_errno;
	int Tid = 0;
	struct tm *tm;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	struct tm tmstruc;
#endif
	time_t current_time;
	int fd_log;

	save_errno = errno;
	va_start (args, msg);
	(void) time (&current_time);		/* Get current time */
#if (defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32)
	(void) localtime_r (&current_time, &tmstruc);
	tm = &tmstruc;
#else
	tm = localtime (&current_time);
#endif
	Cglobals_getTid(&Tid);
	if (Tid < 0)	/* main thread */
		sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		    tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, jid, func);
	else
		sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d,%d %s: ", tm->tm_mon+1,
		    tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, jid, Tid, func);
	vsprintf (prtbuf+strlen(prtbuf), msg, args);
	va_end (args);
	fd_log = open (LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664);
	write (fd_log, prtbuf, strlen(prtbuf));
	close (fd_log);
	errno = save_errno;
	return (0);
}
