/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nslogit.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:22 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "Cglobals.h"
#include "Cns.h"
extern int jid;
extern char logfile[];

nslogit(char *func, char *msg, ...)
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
	Cglobals_getTid (&Tid);
	if (Tid < 0)	/* main thread */
		Csnprintf (prtbuf, LOGBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s: ",
		    tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec,
		    jid, func);
	else
		Csnprintf (prtbuf, LOGBUFSZ, "%02d/%02d %02d:%02d:%02d %5d,%d %s: ",
		    tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec,
		    jid, Tid, func);
	Cvsnprintf (prtbuf+strlen(prtbuf), LOGBUFSZ-strlen(prtbuf), msg, args);
	if (prtbuf[LOGBUFSZ-2] != '\n') {
		prtbuf[LOGBUFSZ-2] = '\n';
		prtbuf[LOGBUFSZ-1] = '\0';
	}
	va_end (args);
	fd_log = open (logfile, O_WRONLY | O_CREAT | O_APPEND, 0664);
	write (fd_log, prtbuf, strlen(prtbuf));
	close (fd_log);
	errno = save_errno;
	return (0);
}
