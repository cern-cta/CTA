/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlflogit.c,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:05:27 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <varargs.h>
#include "Cglobals.h"
#include "dlf.h"

extern int jid;

dlflogit(va_alist) va_dcl
{
	va_list args;
	char *func;
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
	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
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
	if ((fd_log = open (LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) > 0) {
	  write (fd_log, prtbuf, strlen(prtbuf));
	  close (fd_log);
	}
	errno = save_errno;
	return (0);
}
