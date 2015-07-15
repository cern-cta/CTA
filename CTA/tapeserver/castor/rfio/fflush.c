/*
 * $Id: fflush.c,v 1.9 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fflush.c     Remote File I/O - flush a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

/*
 * Remote file flush
 * If the file is remote, this is a dummy operation.
 */
int rfio_fflush(RFILE *fp)
{
  int     status;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fflush(%x)", fp);

  if ( fp == NULL ) {
    errno = EBADF;
    END_TRACE();
    return -1 ;
  }

  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1 ) {
    status= fflush((FILE *)fp) ;
    END_TRACE() ;
    rfio_errno = 0;
    return status ;
  }

  /*
   * Checking magic number
   */
  if ( fp->magic != RFIO_MAGIC) {
    int fps = fp->s;
    serrno = SEBADVERSION ;
    free((char *)fp);
    (void) close(fps) ;
    END_TRACE() ;
    return -1 ;
  }
  END_TRACE();
  return 0 ;
}
