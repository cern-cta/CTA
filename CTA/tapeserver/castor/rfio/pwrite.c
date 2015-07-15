/*
 * $Id: pwrite.c,v 1.10 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1993-2002 by  CERN/IT/PDP/DM
 * All rights reserved
 */

/* pwrite.c     Remote command I/O - write input to a popened command   */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include <stdlib.h>

/* Remote file write   */
int rfio_pwrite(char    *ptr,                           /* buffer pointer               */
                int     size,
                int items,                              /* .. size items                */
                RFILE   *fp)                            /* remote file pointer          */
{
  char    buf[BUFSIZ];       /* General input/output buffer          */
  int status ;
  char *p=buf;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_pwrite(%x, %d, %d, %x)", ptr, size, items, fp);

  if (fp == NULL || fp->magic != RFIO_MAGIC) {
    errno = EBADF;
    if (fp != NULL)
      free((char *)fp);
    END_TRACE();
    return(-1);
  }
  /*
   * File is local
   */
  if (fp->s < 0)  {
    status = fwrite(ptr, size, items, fp->fp_save);
    TRACE(3,"rfio","local pwrite (%x,%d,%d,%x) returns %d",ptr, size, items, &(fp-> fp), status);
    if ( status == 0 ) serrno = 0;
    END_TRACE();
    rfio_errno = 0;
    return(status);
  }

  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_FWRITE);
  marshall_LONG(p, size);
  marshall_LONG(p, items);
  if ( items*size > BUFSIZ ) {
    TRACE(2,"rfio","rfio_pwrite: request too long %d (max %d)",items*size,BUFSIZ);
    END_TRACE();
    serrno = E2BIG;
    return(-1);
  }
  TRACE(2, "rfio", "rfio_pwrite: sending %d bytes", 2*WORDSIZE+2*LONGSIZE);
  if (netwrite_timeout(fp->s, buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE )     {
    TRACE(2,"rfio","rfio_pwrite: write(): ERROR occured (errno=%d)",errno);
    END_TRACE();
    return -1;
  }
  TRACE(2, "rfio", "rfio_pwrite: sending %d bytes", items*size);
  p = buf ;
  marshall_STRING(p,ptr) ;
  if (netwrite_timeout(fp->s, buf, items*size, RFIO_DATA_TIMEOUT) != (items*size))       {
    TRACE(2, "rfio", "rfio_pwrite: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return(-1);
  }
  p = buf;
  TRACE(2, "rfio", "rfio_pwrite: reading %d bytes", 2*LONGSIZE);
  if (netread_timeout(fp->s, buf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
    TRACE(2, "rfio", "rfio_pwrite: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return(-1);
  }
  unmarshall_LONG(p, status);
  unmarshall_LONG(p, rfio_errno);
  TRACE(1, "rfio", "rfio_pwrite: status %d, rfio_errno %d",status,rfio_errno);
  END_TRACE();
  return(status);
}
