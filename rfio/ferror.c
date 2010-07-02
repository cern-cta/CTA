/*
 * $Id: ferror.c,v 1.7 2009/06/03 13:57:05 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* ferror.c      Remote File I/O - tell if an error happened            */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include "rfio_rfilefdt.h"
#include <stdlib.h>

int rfio_ferror(RFILE * fp)
{
  int     rc  ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_ferror(%x)", fp);

  if ( fp == NULL ) {
    errno = EBADF;
    END_TRACE();
    return -1 ;
  }

  /*
   * The file is local
   */
  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1 ) {
    rc= ferror((FILE *)fp) ;
    END_TRACE() ;
    rfio_errno = 0;
    return rc ;
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

  /*
   * The file is remote, using then the eof flag updated
   * by rfio_fread.
   */
#ifdef linux
  if ( ((RFILE *)fp)->eof & _IO_ERR_SEEN )
#else
#ifdef __APPLE__
    if ( ((RFILE *)fp)->eof & __SERR)
#else
      if ( ((RFILE *)fp)->eof & _IOERR )
#endif
#endif
        rc = 1 ;
      else
        rc = 0 ;
  END_TRACE() ;
  return rc ;
}
