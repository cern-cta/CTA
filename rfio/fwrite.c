/*
 * $Id: fwrite.c,v 1.13 2009/06/03 13:57:06 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fwrite.c     Remote File I/O - write a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

/*
 * Remote file buffered write
 */
int DLL_DECL rfio_fwrite(ptr, size, items, fp)
     void    *ptr;          /* buffer pointer */
     int     size, items;
     RFILE   *fp;
{
  int rc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fwrite(%x, %d, %d, %x)", ptr, size, items, fp);

  if (fp == NULL ) {
    errno = EBADF ;
    END_TRACE() ;
    return 0 ;
  }

  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    rc= fwrite(ptr, size, items, (FILE *)fp) ;
    if ( rc == 0 ) serrno = 0;
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
    END_TRACE();
    return 0 ;
  }

  /*
   * The file is remote
   */
  rc= rfio_write(fp->s,ptr,size*items) ;
  switch(rc) {
  case -1:
#ifdef linux
    ((RFILE *)fp)->eof |= _IO_ERR_SEEN ;
#else
#ifdef __APPLE__
    ((RFILE *)fp)->eof |= __SERR ;
#else
    ((RFILE *)fp)->eof |= _IOERR ;
#endif
#endif
    rc= 0 ;
    break ;
  case 0:
#ifdef linux
    ((RFILE *)fp)->eof |= _IO_EOF_SEEN ;
#else
#ifdef __APPLE__
    ((RFILE *)fp)->eof |= __SEOF ;
#else
    ((RFILE *)fp)->eof |= _IOEOF ;
#endif
#endif
    break ;
  default:
    rc= (rc+size-1)/size ;
    break ;
  }
  END_TRACE() ;
  return rc ;
}
