/*
 * $Id: feof.c,v 1.9 2009/06/03 13:57:05 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* feof.c      Remote File I/O - tell if the eof has been reached       */

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

int rfio_feof(RFILE * fp)
{
  int     rc  ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_feof(%x)", fp);

  if ( fp == NULL ) {
    errno = EBADF;
    END_TRACE();
    return -1 ;
  }

  /*
   * The file is local
   */
  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1 ) {
    rc= feof((FILE *)fp) ;
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
  if ( ((RFILE *)fp)->eof & _IO_EOF_SEEN )
#else
#ifdef __APPLE__
    if ( ((RFILE *)fp)->eof & __SEOF )
#else
      if ( ((RFILE *)fp)->eof & _IOEOF )
#endif
#endif
        rc = 1 ;
      else
        rc = 0 ;
  END_TRACE() ;
  return rc ;

}
