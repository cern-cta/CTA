/*
 * $Id: fseek.c,v 1.12 2009/06/03 13:57:05 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fseek.c      Remote File I/O - fseek library call */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

/*
 * Remote file fseek
 */
int rfio_fseek(fp, offset, whence)
     RFILE *fp;
     long int offset;
     int whence;
{
  int rc;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fseek(%x, %d, %d)", fp, offset, whence);

  /*
   * Checking fp validity
   */
  if (fp == NULL) {
    errno = EBADF;
    TRACE(2,"rfio","rfio_fseek() : FILE ptr is NULL ");
    END_TRACE();
    return -1;
  }

  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    TRACE(2,"rfio","rfio_fseek() : using local fseek() ");
    rc = fseek((FILE *)fp, offset, whence);
    if ( rc < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE();
    return rc;
  }

  TRACE(2,"rfio","rfio_fseek() : after remoteio") ;

  /*
   * Checking magic number
   */
  if (fp->magic != RFIO_MAGIC) {
    int fps = fp->s;
    serrno = SEBADVERSION;
    TRACE(2,"rfio","rfio_fseek() : Bad magic number");
    free((char *)fp);
    (void) close(fps);
    END_TRACE();
    return -1;
  }

  /*
   * The file is remote
   */
  rc = rfio_lseek(fp->s,offset,whence);
  switch(rc)
  {
  case -1:
#ifdef linux
    ((RFILE *)fp)->eof |= _IO_ERR_SEEN;
#else
#ifdef __APPLE__
    ((RFILE *)fp)->eof |= __SERR;
#else
    ((RFILE *)fp)->eof |= _IOERR;
#endif
#endif
    break;
  default:
    rc = 0;
    break;
  }
  END_TRACE();
  return rc;
}
