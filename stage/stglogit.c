/*
 * $Id: stglogit.c,v 1.40 2002/09/26 08:59:11 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stglogit.c,v $ $Revision: 1.40 $ $Date: 2002/09/26 08:59:11 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <varargs.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "stage_constants.h"
#include "osdep.h"
#include "Cglobals.h"

extern int reqid;
static int prtbuf_key = -1;

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
	int save_errno;

	save_errno = errno;
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
	errno = save_errno;
	return(0);
}

int stglogtppool(func,tppool)
	char *func;
	char *tppool;
{
	char prtbuf[PRTBUFSZ];
	struct tm *tm;
	time_t current_time;
	int fd_log;

	if ((func == NULL) || (tppool == NULL)) {
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


char *stglogflags(func,logfile,flags)
	char *func;
	char *logfile;
	u_signed64 flags;
{
	char *prtbuf = NULL;
	char *thisp;
	struct tm *tm;
	time_t current_time;
	int fd_log;
	int i;
	int something_to_print;

	struct flag2name flag2names[] = {
		{ STAGE_DEFERRED   , "STAGE_DEFERRED"             },
		{ STAGE_GRPUSER    , "STAGE_GRPUSER"              },
		{ STAGE_COFF       , "STAGE_COFF"                 },
		{ STAGE_UFUN       , "STAGE_UFUN"                 },
		{ STAGE_INFO       , "STAGE_INFO"                 },
		{ STAGE_ALL        , "STAGE_ALL"                  },
		{ STAGE_LINKNAME   , "STAGE_LINKNAME"             },
		{ STAGE_LONG       , "STAGE_LONG"                 },
		{ STAGE_PATHNAME   , "STAGE_PATHNAME"             },
		{ STAGE_SORTED     , "STAGE_SORTED"               },
		{ STAGE_STATPOOL   , "STAGE_STATPOOL"             },
		{ STAGE_TAPEINFO   , "STAGE_TAPEINFO"             },
		{ STAGE_USER       , "STAGE_USER"                 },
		{ STAGE_EXTENDED   , "STAGE_EXTENDED"             },
		{ STAGE_ALLOCED    , "STAGE_ALLOCED"              },
		{ STAGE_FILENAME   , "STAGE_FILENAME"             },
		{ STAGE_EXTERNAL   , "STAGE_EXTERNAL"             },
		{ STAGE_MULTIFSEQ  , "STAGE_MULTIFSEQ"            },
		{ STAGE_MIGRULES   , "STAGE_MIGRULES"             },
		{ STAGE_SILENT     , "STAGE_SILENT"               },
		{ STAGE_NOWAIT     , "STAGE_NOWAIT"               },
		{ STAGE_NOREGEXP   , "STAGE_NOREGEXP"             },
		{ STAGE_DUMP       , "STAGE_DUMP"                 },
		{ STAGE_CLASS      , "STAGE_CLASS"                },
		{ STAGE_QUEUE      , "STAGE_QUEUE"                },
		{ STAGE_COUNTERS   , "STAGE_COUNTERS"             },
		{ STAGE_NOHSMCREAT , "STAGE_NOHSMCREAT"           },
		{ STAGE_CONDITIONAL, "STAGE_CONDITIONAL"          },
		{ STAGE_FORCE      , "STAGE_FORCE"                },
		{ STAGE_REMOVEHSM  , "STAGE_REMOVEHSM"            },
		{ STAGE_RETENP     , "STAGE_RETENP"               },
		{ STAGE_MINTIME    , "STAGE_MINTIME"              },
 		{ STAGE_VERBOSE    , "STAGE_VERBOSE"              },
 		{ STAGE_DISPLAY_SIDE , "STAGE_DISPLAY_SIDE"       },
 		{ STAGE_SIDE       , "STAGE_SIDE"                 },
 		{ STAGE_FILE_ROPEN  , "STAGE_FILE_ROPEN"          },
 		{ STAGE_FILE_RCLOSE , "STAGE_FILE_RCLOSE"         },
 		{ STAGE_FILE_WOPEN  , "STAGE_FILE_WOPEN"          },
 		{ STAGE_FILE_WCLOSE , "STAGE_FILE_WCLOSE"         },
 		{ STAGE_REQID       , "STAGE_REQID"               },
 		{ STAGE_HSM_ENOENT_OK , "STAGE_HSM_ENOENT_OK"     },
 		{ STAGE_NOLINKCHECK , "STAGE_NOLINKCHECK"         },
  		{ STAGE_NORETRY     , "STAGE_NORETRY"             },
  		{ STAGE_VOLATILE_TPPOOL , "STAGE_VOLATILE_TPPOOL" },
  		{ STAGE_NODISK     , "STAGE_NODISK"               },
  		{ STAGE_MIGRINIT   , "STAGE_MIGRINIT"             },
  		{ STAGE_HSMCREAT   , "STAGE_HSMCREAT"             },
  		{ STAGE_FORMAT     , "STAGE_FORMAT"               },
        { 0                , NULL                         }
	};
      
	Cglobals_get(&prtbuf_key, (void**)&prtbuf, (size_t) PRTBUFSZ);
	if (prtbuf == NULL) return(NULL);

	prtbuf[0] = '\0';
	if (func != NULL) {
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
	}
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
	if ((something_to_print != 0) && (logfile != NULL)) {
		strcat(prtbuf, "\n");
		if ((fd_log = open (logfile, O_WRONLY | O_CREAT | O_APPEND, 0664)) >= 0) {
			write (fd_log, prtbuf, strlen(prtbuf));
			close (fd_log);
		}
	}
	return(prtbuf);
}

int stglogopenflags(func,flags)
	char *func;
	u_signed64 flags;
{
	char prtbuf[PRTBUFSZ];
	char *thisp;
	struct tm *tm;
	time_t current_time;
	int fd_log;
	int i;
	int something_to_print;

	struct flag2name flag2names[] = {
#ifdef O_RDONLY
		{ O_RDONLY    , "O_RDONLY"    },
#endif
#ifdef O_WRONLY
		{ O_WRONLY    , "O_WRONLY"    },
#endif
#ifdef O_RDWR
		{ O_RDWR      , "O_RDWR"      },
#endif
#ifdef O_CREAT
		{ O_CREAT     , "O_CREAT"     },
#endif
#ifdef O_EXCL
		{ O_EXCL      , "O_EXCL"      },
#endif
#ifdef O_NOCTTY
		{ O_NOCTTY    , "O_NOCTTY"    },
#endif
#ifdef O_TRUNC
		{ O_TRUNC     , "O_TRUNC"     },
#endif
#ifdef O_APPEND
		{ O_APPEND    , "O_APPEND"    },
#endif
#ifdef O_NONBLOCK
		{ O_NONBLOCK  , "O_NONBLOCK"  },
#endif
#ifdef O_NDELAY
		{ O_NDELAY    , "O_NDELAY"    },
#endif
#ifdef O_SYNC
		{ O_SYNC      , "O_SYNC"      },
#endif
#ifdef O_NOFOLLOW
		{ O_NOFOLLOW  , "O_NOFOLLOW"  },
#endif
#ifdef O_DIRECTORY
		{ O_DIRECTORY , "O_DIRECTORY" },
#endif
#ifdef O_LARGEFILE
		{ O_LARGEFILE , "O_LARGEFILE" },
#endif
        { 0                , NULL     }
	};
      
	if (func == NULL) {
		return(-1);
	}

	/* Buffersize all the flags */
	time (&current_time);		/* Get current time */
	tm = localtime (&current_time);
#if (defined(__osf__) && defined(__alpha))
	sprintf (prtbuf, "%02d/%02d %02d:%02d:%02d %5d %s openflags: ", tm->tm_mon+1,
			 tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#else
	snprintf (prtbuf, PRTBUFSZ, "%02d/%02d %02d:%02d:%02d %5d %s openflags: ", tm->tm_mon+1,
			  tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, reqid, func);
#endif
	prtbuf[PRTBUFSZ-1] = '\0';
	thisp = prtbuf + strlen(prtbuf);
	i = -1;
	something_to_print = 0;
    while (1) {
		if (flag2names[++i].name == NULL) break;
		/* We distinguish the case when flag2names[i].flag == 0 v.s. the others */
		if ((flag2names[i].flag == 0) && (flags != 0)) continue;
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
	int save_errno;

	save_errno = errno;
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
	errno = save_errno;
	return(0);
}

