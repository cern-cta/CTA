/*
 * $Id: ftell.c,v 1.4 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by IN2P3 CC Philippe Gaillardon
 * All rights reserved
 */

/* ftell.c      Remote File I/O - get current file position. */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

long rfio_ftell(RFILE    *fp)
{
  long      rc;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_ftell(%x)", fp);


  /*
   * Checking fp validity
   */
  if (fp == NULL) {
    errno = EBADF;
    TRACE(2,"rfio","rfio_ftell() : FILE ptr is NULL ");
    END_TRACE();
    return -1;
  }

  /*
   * The file is local : this is the only way to detect it !
   */
  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    TRACE(2,"rfio","rfio_ftell() : using local ftell() ");
    rc = ftell((FILE *)fp);
    if ( rc < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE();
    return rc;
  }

  TRACE(2,"rfio","rfio_ftell() : after remoteio") ;

  /*
   * Checking magic number
   */
  if (fp->magic != RFIO_MAGIC) {
    int fps = fp->s;
    serrno = SEBADVERSION;
    TRACE(2,"rfio","rfio_ftell() : Bad magic number");
    free((char *)fp);
    (void) close(fps);
    END_TRACE();
    return -1;
  }

  /* Just use rfio_lseek                                 */
  rc = rfio_lseek(fp->s, 0, SEEK_CUR);
  END_TRACE();
  return rc;
}
