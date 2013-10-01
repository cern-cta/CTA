/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include "h/rmc.h"
extern int jid;

int rmclogit(const char *const func, const char *const msg, ...)
{
	va_list args;
	char prtbuf[RMC_PRTBUFSZ];
	int save_errno;
	struct tm *tm;
	time_t current_time;
	int fd_log;

	save_errno = errno;
	va_start (args, msg);
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, jid, func);
	vsprintf (prtbuf+strlen(prtbuf), msg, args);
	va_end (args);
	fd_log = open("/var/log/castor/rmcd_legacy.log", O_WRONLY | O_CREAT | O_APPEND, 0664);
        if (fd_log < 0) return -1;        
	write (fd_log, prtbuf, strlen(prtbuf));
	close (fd_log);
	errno = save_errno;
	return (0);
}
