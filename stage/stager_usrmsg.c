/*
 * $Id: stager_usrmsg.c,v 1.1 2000/03/23 00:37:24 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: stager_usrmsg.c,v $ $Revision: 1.1 $ $Date: 2000/03/23 00:37:24 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

/* stager_usrmsg.c - callback rtcp routine */

#if defined(_WIN32)
#include <io.h>
#include <pwd.h>
#endif /* _WIN32 */

#include <stdlib.h>
#include <stdio.h>              /* standard input/output definitions    */
#include <string.h>
#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
#include <varargs.h>            /* variable argument list definitions   */
#else
#include <stdarg.h>             /* variable argument list definitions   */
#endif /* IRIX5 || __Lynx__ */
#include <log.h>                /* logging options and definitions      */
#include <stage.h>              /* FOR MSG_ERR MSG_OUT constants        */

extern int rpfd;
extern int sendrep();

/*
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
	vsprintf(line,format,args);
	sendrep(rpfd,(level == LOG_ERR) ? MSG_ERR : MSG_OUT,line,strlen(line)) ;
	va_end(args);
}




