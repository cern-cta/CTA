/*
 * $Id: stglogit.c,v 1.19 2001/03/09 10:58:27 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit.c,v $ $Revision: 1.19 $ $Date: 2001/03/09 10:58:27 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <varargs.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "stage.h"
#include "stage_api.h"
#include "osdep.h"

extern int reqid;

struct flag2name {
	u_signed64 flag;
	char *name;
};

int stglogit(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;
	int fd_log;

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
	if ((fd_log = open (LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) >= 0) {
		write (fd_log, prtbuf, strlen(prtbuf));
		close (fd_log);
	}
	return(0);
}

int stglogtppool(va_alist) va_dcl
{
	va_list args;
	char *func;
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;
	int fd_log;
	char *tppool;

	va_start (args);
	func = va_arg (args, char *);
	tppool = va_arg (args, char *);
	va_end (args);

	if (tppool == NULL) {
		return(-1);
	}
	if (tppool[0] == '\0') {
		return(-1);
	}

	/* Buffersize all the flags */
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
#if (defined(__osf__) && defined(__alpha))
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s tppool: %s\n", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func, tppool);
#else
	snprintf (prtbuf, PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s tppool: %s\n", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func, tppool);
#endif
	prtbuf[PRTBUFSZ-1] = '\0';
	if ((fd_log = open (LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) >= 0) {
		write (fd_log, prtbuf, strlen(prtbuf));
		close (fd_log);
	}
	return(0);
}


int stglogflags(va_alist) va_dcl
{
	va_list args;
	char *func;
	u_signed64 flags;
	char prtbuf[PRTBUFSZ];
	char *thisp;
	struct tm *tm;
	time_t current_time;
	int fd_log;
	int i;
	int something_to_print;

	struct flag2name flag2names[] = {
		{ STAGE_DEFERRED  , "STAGE_DEFERRED"  },
		{ STAGE_GRPUSER   , "STAGE_GRPUSER"   },
		{ STAGE_COFF      , "STAGE_COFF"      },
		{ STAGE_UFUN      , "STAGE_UFUN"      },
		{ STAGE_INFO      , "STAGE_INFO"      },
		{ STAGE_ALL       , "STAGE_ALL"       },
		{ STAGE_LINKNAME  , "STAGE_LINKNAME"  },
		{ STAGE_LONG      , "STAGE_LONG"      },
		{ STAGE_PATHNAME  , "STAGE_PATHNAME"  },
		{ STAGE_SORTED    , "STAGE_SORTED"    },
		{ STAGE_STATPOOL  , "STAGE_STATPOOL"  },
		{ STAGE_TAPEINFO  , "STAGE_TAPEINFO"  },
		{ STAGE_USER      , "STAGE_USER"      },
		{ STAGE_EXTENDED  , "STAGE_EXTENDED"  },
		{ STAGE_ALLOCED   , "STAGE_ALLOCED"   },
		{ STAGE_FILENAME  , "STAGE_FILENAME"  },
		{ STAGE_EXTERNAL  , "STAGE_EXTERNAL"  },
		{ STAGE_MULTIFSEQ , "STAGE_MULTIFSEQ" },
		{ STAGE_MIGRULES  , "STAGE_MIGRULES"  },
		{ STAGE_SILENT    , "STAGE_SILENT"    },
		{ STAGE_NOWAIT    , "STAGE_NOWAIT"    },
		{ STAGE_CLASS     , "STAGE_CLASS"     },
		{ STAGE_QUEUE     , "STAGE_QUEUE"     },
		{ STAGE_COUNTERS  , "STAGE_COUNTERS"  },
        { 0               , NULL              }
	};
      
	va_start (args);
	func = va_arg (args, char *);
	flags = va_arg (args, u_signed64);
	va_end (args);

	/* Buffersize all the flags */
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
#if (defined(__osf__) && defined(__alpha))
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s flags: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#else
	snprintf (prtbuf, PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s flags: ", tm->tm_mon+1,
		tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#endif
	prtbuf[PRTBUFSZ-1] = '\0';
	thisp = prtbuf + strlen(prtbuf);
	i = -1;
	something_to_print = 0;
    while (1) {
		if (flag2names[++i].name == NULL) break;
		if ((flags & flag2names[i].flag) == flag2names[i].flag) {
			if (strlen(prtbuf) > (PRTBUFSZ - 3)) break;
			if (*thisp != '\0') {
				strcat(prtbuf, "|");
			}
			if (strlen(prtbuf) > (PRTBUFSZ - 1 - strlen(flag2names[i].name) - 1)) break;
			strcat(prtbuf, flag2names[i].name);
			something_to_print = 1;
		}
	}
	if (something_to_print != 0) {
		strcat(prtbuf, "\n");
		if ((fd_log = open (LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) >= 0) {
			write (fd_log, prtbuf, strlen(prtbuf));
			close (fd_log);
		}
	}
	return(0);
}

int stgmiglogit(va_alist) va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;
	int fd_log;

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
	if ((fd_log = open (MIGLOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0664)) >= 0) {
		write (fd_log, prtbuf, strlen(prtbuf));
		close (fd_log);
	}
	return(0);
}

