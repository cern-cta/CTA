/*
 * $Id: stager_usrmsg.c,v 1.17 2002/08/27 08:38:04 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_usrmsg.c,v $ $Revision: 1.17 $ $Date: 2002/08/27 08:38:04 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

/* stager_usrmsg.c - callback rtcp routine */

#include <stdio.h>
#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif /* _WIN32 */
#include <pwd.h>
#include <sys/types.h>

#include <stdlib.h>
#include <stdio.h>              /* standard input/output definitions    */
#include <string.h>
#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
#include <varargs.h>            /* variable argument list definitions   */
#else
#include <stdarg.h>             /* variable argument list definitions   */
#endif /* IRIX5 || __Lynx__ */
#include <log.h>                /* logging options and definitions      */
#include "stage_api.h"          /* For seteuid/setegid macro on hpux */
#include "osdep.h"

extern int rpfd;
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int *, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stgmiglogit _PROTO(());
extern struct passwd start_passwd;

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#define SETEID(thiseuid,thisegid) {              \
	setegid(start_passwd.pw_gid);                \
	seteuid(start_passwd.pw_uid);                \
	setegid(thisegid);                           \
	seteuid(thiseuid);                           \
}

/*
 * Version for explicit migration - Calls sendrep
 *
 * stager_usrmsg should be called with the following syntax
 * stager_usrmsg(LOG_LEVEL,format[,value,...]) ;
 */

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
void stager_usrmsg(va_alist)     va_dcl
#else
void stager_usrmsg(int level, ...)
#endif
{
	va_list args ;          /* Variable argument list               */
	char    *format;        /* Format of the log message            */
	char    line[BUFSIZ] ;  /* Formatted log message                */
#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
	int     level;          /* Level of the message                 */

	va_start(args);         /* initialize to beginning of list      */
	level = va_arg(args, int);
#else

	va_start(args, level);
#endif /* IRIX5 || __Lynx__ */

	format = va_arg(args, char *);
#if (defined(__osf__) && defined(__alpha))
	vsprintf(line,format,args);
#else
	vsnprintf(line,BUFSIZ,format,args);
#endif
	line[BUFSIZ-1] = '\0';
#ifdef STAGER_DEBUG
    /* In debug mode - we always want to have all messages in stager log-file */
	sendrep(&rpfd,MSG_ERR,"%s",line) ;
#else
	if (level != LOG_DEBUG) sendrep(&rpfd,RTCOPY_OUT,"%s",line) ;
#endif
	va_end(args);
}

/*
 * Version for automatic migration - Calls stgmiglogit
 *
 * stager_migmsg should be called with the following syntax
 * stager_migmsg(LOG_LEVEL,format[,value,...]) ;
 */

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
void stager_migmsg(va_alist)     va_dcl
#else
void stager_migmsg(int level, ...)
#endif
{
	va_list args ;          /* Variable argument list               */
	char    *format;        /* Format of the log message            */
	char    line[BUFSIZ] ;  /* Formatted log message                */
	int save_euid, save_egid;

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
	int     level;          /* Level of the message                 */

	va_start(args);         /* initialize to beginning of list      */
	level = va_arg(args, int);
#else

	va_start(args, level);
#endif /* IRIX5 || __Lynx__ */

	format = va_arg(args, char *);
#if (defined(__osf__) && defined(__alpha))
	vsprintf(line,format,args);
#else
	vsnprintf(line,BUFSIZ,format,args);
#endif
	line[BUFSIZ-1] = '\0';
	save_euid = geteuid();
	save_egid = getegid();
	SETEID(0,0);
#ifdef STAGER_DEBUG
    /* In debug mode - we always want to have all messages in stager log-file */
	stgmiglogit("migrator","%s",line);
#else
	if (level != LOG_DEBUG) stgmiglogit("migrator","%s",line);
#endif
	SETEID(save_euid,save_egid);
	va_end(args);
}




