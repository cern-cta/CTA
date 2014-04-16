/*
 * $Id: symlink.c,v 1.15 2008/07/31 07:09:14 sponcec3 Exp $
 */


/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define RFIO_KERNEL     1
#include "rfio.h"
#include <Cpwd.h>

#include <errno.h>
#include <string.h>
#include <stdlib.h>            /* malloc prototype */

/*
 * returns -1 if an error occured or no information is available at the
 * daemon's host. 0 otherwise.
 */
int rfio_symlink(char *n1,
                 char *n2)
{
  int status ;
  char *host ;
  char * filename;
  int parserc ;
  /*
   * The file is local.
   */
  INIT_TRACE("RFIO_TRACE");
  TRACE( 1, "rfio", " rfio_symlink (%s,%s)",n1,n2 );
  if ( ! (parserc = rfio_parseln(n2,&host,&filename,NORDLINKS)) ) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file.
       */
      TRACE(1,"rfio","rfio_symlink: %s is an HSM path",
            filename);
      END_TRACE();
      rfio_errno = 0;
      serrno = SEOPNOTSUP;
      status = -1;
      return(status);
    }
    TRACE(2,"rfio","rfio_symlink local %s -> %s",filename,n1);
    status = symlink(n1,filename) ;
    if ( status < 0 ) serrno = 0;
    END_TRACE() ;
    rfio_errno = 0;
    return status ;
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(1,"rfio","rfio_symlink: failure, error %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return -1;
}
