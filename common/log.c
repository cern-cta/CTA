/*
 * Copyright (C) 1990-2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* log.c        - generalized logging routines                          */

#include <unistd.h>

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>              /* standard input/output definitions    */
#include <string.h>
#include <fcntl.h>              /* file control                         */
#include <time.h>               /* time related definitions             */
#include <stdarg.h>             /* variable argument list definitions   */
#include <errno.h>              /* standard error numbers & codes       */
#include <log.h>                /* logging options and definitions      */
#include <Cglobals.h>           /* Thread globals. Get Thread ID.       */


static mode_t logbits=0666;
static int loglevel=LOG_NOLOG;  /* logging level                        */
static char logname[64];        /* logging facility name                */
static char logfilename[64]=""; /* log file name                        */
static char strftime_format[] = "%b %e %H:%M:%S";

static int pid;                 /* process identifier                   */

extern char *getenv();

void (*logfunc) (int, char *, ...)=(void (*) (int, char *, ...))logit;
void setlogbits (int);

/*
 * Opening log file.
 * Storing the process pid.
 */
void initlog(
  const char *const name,  /* facility name    */
  const int        level,  /* logging level    */
  char *const      output) /* output specifier */
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

  /*
   * Opening log file and setting logfunc
   * to either syslog or logit.
   */
  if (!strcmp(output,"syslog"))   {
    logfunc=(void (*) (int, char *, ...))syslog;
  } else {
    logfunc=(void (*) (int, char *, ...))logit;
    if (0 != strlen(output)) {
      strcpy(logfilename,output);
    }
  }
}

/*
 * Mode bits for log file.
 */
void setlogbits(int bits)                  /* logfile mode bits */
{
  logbits=(mode_t) bits;
}

/*
 * logit should be called with the following syntax
 * logit(LOG_LEVEL,format[,value,...]) ;
 */
void logit(int level, char *format, ...)
{
  va_list args ;          /* Variable argument list               */
  time_t  clock;
  struct tm tmstruc;
  struct tm *tp;
  char    timestr[64] ;   /* Time in its ASCII format             */
  char    line[BUFSIZ] ;  /* Formatted log message                */
  int     Tid = 0;        /* Thread ID if MT                      */
  int     save_errno;

  save_errno = errno;
  va_start(args, format);
  if (loglevel >= level)  {
    (void) time(&clock);
    (void)localtime_r(&clock,&tmstruc);
    tp = &tmstruc;
    (void) strftime(timestr,64,strftime_format,tp);
    Cglobals_getTid(&Tid);
    if ( Tid < 0 ) {
      /*
       * Non-MT or main thread. Don't print thread ID.
       */
      pid = getpid();
      (void) sprintf(line,"%s %s[%d]: ",timestr,logname,pid) ;
    } else {
      pid = getpgrp();
      (void) sprintf(line,"%s %s[%d,%d]: ",timestr,logname,pid,Tid) ;
    }
    (void) vsprintf(line+strlen(line),format,args);
    if (0 != strlen(logfilename)) {
      const int fd = open(logfilename,O_CREAT|O_WRONLY| O_APPEND,logbits);
      if ( -1 == fd ) {
        syslog(LOG_ERR,"open: %s", logfilename);
        /* FH we probably should retry */
        va_end(args);
        errno = save_errno;
        return;
      } else {
        /*
         * Write log message mahing sure file access is enabled
         * even if root umask is not 0
         */
        (void) chmod(logfilename, logbits);
        (void) write(fd,line,strlen(line));
        (void) close(fd);
      }
    }
  }
  va_end(args);
  errno = save_errno;
}

int getloglv()
{
  return(loglevel);
}
