/*
 * $Id: ftello64.c,v 1.3 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* ftello64.c      Remote File I/O - get current file position. */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include "u64subr.h"
#include <stdlib.h>

/*
 * Remote file ftello
 */
off64_t rfio_ftello64(RFILE    *fp)
{
  off64_t  offsetout;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_ftello64(%x)", fp);


  /*
   * Checking fp validity
   */
  if (fp == NULL) {
    errno = EBADF;
    TRACE(2,"rfio","rfio_ftello64() : FILE ptr is NULL ");
    END_TRACE();
    return -1;
  }

  /*
   * The file is local : this is the only way to detect it !
   */
  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    TRACE(2,"rfio","rfio_ftello64() : using local ftello64() ");
    offsetout = ftello64((FILE *)fp);
    if ( offsetout < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE();
    return offsetout;
  }

  TRACE(2,"rfio","rfio_ftello64() : after remoteio") ;

  /*
   * Checking magic number
   */
  if (fp->magic != RFIO_MAGIC) {
    int fps = fp->s;
    serrno = SEBADVERSION;
    TRACE(2,"rfio","rfio_ftello64() : Bad magic number");
    free((char *)fp);
    (void) close(fps);
    END_TRACE();
    return -1;
  }

  /* Just use rfio_lseek64                                 */
  offsetout = rfio_lseek64(fp->s, (off64_t)0, SEEK_CUR);
  END_TRACE();
  return offsetout;
}
