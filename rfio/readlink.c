/*
 * $Id: readlink.c,v 1.13 2008/07/31 07:09:13 sponcec3 Exp $
 */


/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define RFIO_KERNEL     1
#include "rfio.h"
#include <string.h>
#include <unistd.h>

int rfio_readlink(char *path,
                  char *buf,
                  int length)
{
  int status ;
  char *host ;
  char * filename;
  int parserc;

  /*
   * The file is local.
   */
  INIT_TRACE("RFIO_TRACE");
  TRACE( 1, "rfio", " rfio_readlink (%s,%x,%d)",path,buf,length);
  if ( ! (parserc = rfio_parseln(path,&host,&filename,NORDLINKS)) ) {
    status = readlink(filename,buf,length) ;
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
  TRACE(1,"rfio","rfio_readlink(): rcode = %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return -1;
}
