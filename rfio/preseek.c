/*
 * $Id: preseek.c,v 1.13 2008/07/31 13:16:54 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* preseek.c      Remote File I/O - preseeking.  */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#ifdef linux
#include <sys/uio.h>
#endif
#include "rfio.h"
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

#ifndef min
#define min(a,b) (((a)<(b)) ? (a):(b))
#endif

/*
 * Remote file seek
 */
int rfio_preseek(int      s,
                 struct iovec *iov,
                 int  iovnb)
{
  int s_index;

  INIT_TRACE("RFIO_TRACE") ;
  TRACE(1,"rfio","rfio_preseek(%d, %x, %d)",s,iov,iovnb) ;
  /*
   * The file is local.
   * Nothing has to be done.
   */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1 ) {
    END_TRACE() ;
    return 0 ;
  }

  // Remote directory. Not supported anymore
  rfio_errno = SEOPNOTSUP;
  TRACE(1,"rfio","rfio_preseek: status %d", SEOPNOTSUP) ;
  END_TRACE() ;
  return -1 ;
}
