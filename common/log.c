/*
 * $Id: log.c,v 1.5 2000/02/01 13:13:03 obarring Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: log.c,v $ $Revision: 1.5 $ $Date: 2000/02/01 13:13:03 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* log.c        - generalized logging routines                          */

#if defined(_WIN32)
#include <io.h>
#include <pwd.h>
#endif /* _WIN32 */

#include <stdlib.h>

#include <stdio.h>              /* standard input/output definitions    */
#include <string.h>
#include <fcntl.h>              /* file control                         */
#include <time.h>               /* time related definitions             */

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
#include <varargs.h>            /* variable argument list definitions   */
#else
#include <stdarg.h>             /* variable argument list definitions   */
#include <errno.h>              /* standard error numbers & codes       */
#endif /* IRIX5 || __Lynx__ */
#include <log.h>                /* logging options and definitions      */
#include <Cglobals.h>           /* Thread globals. Get Thread ID.       */


static int loglevel=LOG_NOLOG;  /* logging level                        */
static char logname[64];        /* logging facility name                */
static char logfilename[64]=""; /* log file name                        */
#if defined(_WIN32)
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

static int pid;                 /* process identifier                   */
static int logfd ;              /* logging file descriptor              */
#if !defined(SOLARIS) && !defined(IRIX5) && !defined(HPUX10) && \
    !defined(linux) && !defined(AIX42) && !defined(__Lynx__) && \
    !defined(_WIN32)
extern int syslog();
#endif /* !SOLARIS && !IRIX5 && ... */

#if !defined(_WIN32)
extern char *getenv();
#else  /* _WIN32 */
uid_t getpid();
#endif /* _WIN32 */

#if !defined(IRIX5) && !defined(__Lynx__)
void     logit();
#else
void     logit(int level, ...);
#endif /* IRIX5 || __Lynx__ */
void (*logfunc)()=(void (*)())logit;

/*
 * Opening log file.
 * Storing the process pid.
 */
void initlog(name, level, output)
char    *name;                  /* facility name                        */
int     level;                  /* logging level                        */
char    *output;                /* output specifier                     */
{
    register char  *p;

    loglevel=level;

    /* bypass level if set in environment */
    if ((p = getenv("LOG_PRIORITY")) != NULL) {
        loglevel=atoi(p);
    }
    /*
     * Opening syslog path.
     */
    strcpy(logname,name);
#if defined(sun) || defined(sgi) || defined(hpux) || defined(_AIX)
    openlog(logname, LOG_PID, LOG_USER);
#endif  /* sun || sgi || hpux || _AIX */

    /*
     * Opening log file and setting logfunc
     * to either syslog or logit.
     */
    if (!strcmp(output,"syslog"))   {
        logfunc=(void (*)())syslog;
    } else {
        logfunc=(void (*)())logit;
        if (strlen(output) == 0) {
            logfd= fileno(stderr) ; /* standard error       */
        } else {
            strcpy(logfilename,output);        
        }    
    }
}

/*
 * logit should be called with the following syntax
 * logit(LOG_LEVEL,format[,value,...]) ;
 */
#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
void logit(va_alist)     va_dcl
#else
void logit(int level, ...)
#endif
{
    va_list args ;          /* Variable argument list               */
    char    *format;        /* Format of the log message            */
    time_t  clock;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
    struct tm *tp;
    char    timestr[64] ;   /* Time in its ASCII format             */
    char    line[BUFSIZ] ;  /* Formatted log message                */
    int     fd;             /* log file descriptor                  */
    int     Tid = 0;        /* Thread ID if MT                      */
#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
    int     level;          /* Level of the message                 */

    va_start(args);         /* initialize to beginning of list      */
    level = va_arg(args, int);
#else

    va_start(args, level);
#endif /* IRIX5 || __Lynx__ */
    if (loglevel >= level)  {
        format = va_arg(args, char *);
        (void) time(&clock);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
        (void)localtime_r(&clock,&tmstruc);
        tp = &tmstruc;
#else
        tp = localtime(&clock);
#endif /* _REENTRANT || _THREAD_SAFE */
        (void) strftime(timestr,64,strftime_format,tp);
        Cglobals_getTid(&Tid);
        if ( Tid < 0 ) {
            /*
             * Non-MT or main thread. Don't print thread ID.
             */
            pid = getpid();
            (void) sprintf(line,"%s %s[%d]: ",timestr,logname,pid) ;
        } else {
#if defined(linux)
            pid = getppid();
#else /* linux */
            pid = getpid();
#endif /* linux */
            (void) sprintf(line,"%s %s[%d,%d]: ",timestr,logname,pid,Tid) ;
        }
        (void) vsprintf(line+strlen(line),format,args);
        if (strlen(logfilename)!=0) {
            if ( (fd= open(logfilename,O_CREAT|O_WRONLY|
                O_APPEND,0666)) == -1 ) {
                syslog(LOG_ERR,"open: %s: %m", logfilename);
                /* FH we probably should retry */
                return;
            } else
                /*
                 * To be sure that file access is enables
                 * even if root umask is not 0 
                 */
                 (void) chmod( logfilename, 0666 );           
        } else {
            if  (strlen(logfilename)==0)
                fd= fileno (stderr); /* standard error */
        }
        (void) write(fd,line,strlen(line)) ;
        if (strlen(logfilename)!=0) (void) close(fd);
    }
    va_end(args);
}
        
int getloglv()
{
    return(loglevel);
}
