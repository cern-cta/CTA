/*
 * $Id: popen.c,v 1.15 2009/06/03 13:47:56 sponcec3 Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* popen.c       Remote pipe I/O - open file a file                      */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
extern RFILE *rfilefdt[MAXRFD] ;

RFILE *rfio_popen(char * rcom,
                  char *type)
{

  char *host  ;
  RFILE *rfp  ;
  int rfp_index;
  char *p , *cp, *cp2    ;
  char command[MAXCOMSIZ]; /* command with remote syntax */
  struct passwd *pwuid ;
  char *pcom = 0;
  int rt   ; /* daemon is in the site or remote ? */
  int rcode, status = 0 ;
  int len  ;
  FILE *file, *popen()  ;
  char localhost[MAXHOSTNAMELEN];
  char buf[BUFSIZ] ;

  INIT_TRACE("RFIO_TRACE");

  if ( (int)strlen(rcom) > MAXCOMSIZ -5 ) {
    serrno = SEUMSG2LONG ;
    return NULL ;
  }

  /*
   * get the stderr stream if any
   */
  strcpy(command, rcom) ;
  strcat (command, " 2>&1");
  /*
   * Allocate and initialize a remote file descriptor.
   */
  if ((rfp = (RFILE *)malloc(sizeof(RFILE))) == NULL)        {
    TRACE(2, "rfio", "rfio_popen: malloc(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return NULL ;
  }
  rfio_setup(rfp) ;
  TRACE(3,"rfio","Allocated buffer at %x",rfp);
  cp = strchr(command,':') ;
  cp2 = strchr(command,' ') ;

  /* Bug fix when having a : in the command line but the command is local */
  /* If the first space is before the ':', the command is really local */
  if (cp2 < cp)
    cp = NULL;

  if (cp != NULL) {
    *cp = '\0' ;
    host = command ;
    pcom =  cp + 1 ;
  }
  if ( gethostname(localhost, MAXHOSTNAMELEN) < 0) {
    TRACE(2,"rfio","gethostname() failed");
    TRACE(2,"rfio","freeing RFIO descriptor at 0X%X", rfp);
    (void) free((char *)rfp);
    END_TRACE();
    return NULL;
  }

  /*
   * file is local
   */
  if ( (cp == NULL) || !strcmp( host, localhost) || !strcmp(host,"localhost") ) {
    TRACE(3,"rfio","popen(%s,%s): local mode",command,type) ;
    if (cp == NULL)
    file = popen(command,type);
    else
    file = popen(pcom, type) ;
    rfio_errno = 0;
    if ( file == NULL ) {
      TRACE(1,"rfio","popen() failed ,error %d", errno) ;
      TRACE(2,"rfio","freeing RFIO descriptor at 0X%X", rfp);
      serrno = 0;
      (void) free((char *)rfp);
      END_TRACE();
      return (NULL) ;
    }
    rfp->fp_save = file;
    memcpy( &(rfp->fp), file, sizeof(FILE))  ;
    return ( rfp ) ;
  }

  // Remote call. Not supported anymore
  rfio_errno= SEOPNOTSUP;
  END_TRACE();
  return NULL;
}
