/*
 * $Id: stager_usrmsg.c,v 1.20 2003/09/08 16:56:23 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_usrmsg.c,v $ $Revision: 1.20 $ $Date: 2003/09/08 16:56:23 $ CERN/IT/PDP/DM Jean-Damien Durand";
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
#include <stdarg.h>             /* variable argument list definitions   */
#include <log.h>                /* logging options and definitions      */
#include "stage_api.h"          /* For seteuid/setegid macro on hpux */
#include "osdep.h"

extern int rpfd;
extern int sendrep _PROTO((int *, int, ...));
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

void stager_usrmsg(int level, char *format, ...) {
	va_list args ;          /* Variable argument list               */
	char    line[BUFSIZ] ;  /* Formatted log message                */

	va_start(args, format);

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
