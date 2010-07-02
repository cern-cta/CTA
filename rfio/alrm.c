/*
 * $Id: alrm.c,v 1.7 2009/07/23 12:22:04 waldron Exp $
 */

/*
 * Copyright (C) 1995-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "log.h"
#include <stdlib.h>
#include <unistd.h>
#include <alrm.h>

#ifndef RFIO_ALRM
#define RFIO_ALRM "/var/spool/rfio/rfio_alrm"
#endif
#ifndef LOGSIZELIM
#define LOGSIZELIM 102400
#endif

extern char* getconfent () ;
/*
 * Alarm: rfio writes in an alarm file
 * the buffer. The file should not exist
 * if there is no problem.
 * Returns 0 if an alarm is successfully raised,
 *    1 if no alarm is requested in getconfent().
 *     2 if size of log file is already at its limit
 *     -1 otherwise.
 * If RFIOD ALARM  is 0, alarm for any code.
 */
int rfio_alrm(int rcode,
              char *buf)
{
  time_t clock ;
  int fd  ;
  int n ;
  char buffer[256];
  char *p ;
  int wrtbanner = 0 ;
  struct stat statb ;

  if ( (p= (char *)getconfent("RFIOD","ALRM",0)) == NULL ) {
    log(LOG_DEBUG,"rfio_alrm() entered: no alarm in getconfent() \n");
    return 1 ;
  }
  if ( ( (n=atoi(p)) > 0 && n==rcode ) || n==0  ) {
    time(&clock) ;
    log(LOG_DEBUG,"rfio_alrm(): alarm %s\n",buf) ;
    if ( stat(RFIO_ALRM,&statb) < 0 )  {
      wrtbanner ++ ;
    }
    else {
      if (statb.st_size > LOGSIZELIM)
        return 2 ;
    }

    p=ctime(&clock) ;
    p[strlen(p)-7]='\0' ;
    sprintf(buffer,"%lu\t%d\t%s\t%s\n",clock,rcode,p,buf ) ;

    fd=open(RFIO_ALRM,O_CREAT|O_WRONLY|O_APPEND,0644) ;
    if ( fd < 0 )
      return -1 ;

    if ( wrtbanner ) {
      char banner[64] ;
      sprintf(banner,"Time counter\tError #\tDate\tMessage\n") ;
      write (fd,banner,strlen(banner));
    }
    write(fd,buffer,strlen(buffer)+1) ;
    close(fd);
    return 0 ;
  }
  return -1 ;

}
