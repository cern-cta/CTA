/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include "Cglobals.h"
#include "expert.h"
#include <unistd.h>

extern int jid;

int explogit(const char *args, ...)
{
	va_list ap;
	char *msg;
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
	va_start (ap, args);
	msg = va_arg (ap, char *);
	(void) time (&current_time);		/* Get current time */
#if (defined(_REENTRANT) || defined(_THREAD_SAFE))
	(void) localtime_r (&current_time, &tmstruc);
	tm = &tmstruc;
#else
	tm = localtime (&current_time);
#endif
	Cglobals_getTid(&Tid);
	if (Tid < 0)	/* main thread */
		sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		    tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, jid, args);
	else
		sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d,%d %s: ", tm->tm_mon+1,
		    tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, jid, Tid, args);
	vsprintf (prtbuf+strlen(prtbuf), msg, ap);
	va_end (ap);
	if ((fd_log = open (EXPERTLOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) > -1) {
	  write (fd_log, prtbuf, strlen(prtbuf));
	  close (fd_log);
	}
	errno = save_errno;
	return (0);
}
