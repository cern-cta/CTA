/*
 * Copyright (C) 1998 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fseek.c	1.5 04/06/99 CERN IT-PDP/DM Fabien Collin ";
#endif /* not lint */

/* fseek.c      Remote File I/O - fseek library call */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h"    

/*
 * Remote file fseek
 */
int rfio_fseek(fp, offset, whence)  
     RFILE *fp;
     long int offset;
     int whence;
{
  int rc;
  int remoteio = 0;
  int i;
  
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

  /*
   * The file is local : this is the only way to detect it !
   */
  for (i = 0; i < MAXRFD; i++) {
    if (rfilefdt[i] == fp) { 
      remoteio++;
      break;
    }
  }
	
  if (!remoteio) {
    TRACE(2,"rfio","rfio_fseek() : using local fseek() ");
    rc = fseek((FILE *)fp, offset, whence);
    rfio_errno = 0;
    END_TRACE(); 
    return rc;
  }

  TRACE(2,"rfio","rfio_fseek() : after remoteio") ;

  /*
   * Checking magic number
   */
  if (fp->magic != RFIO_MAGIC) {
    serrno = SEBADVERSION; 
    TRACE(2,"rfio","rfio_fseek() : Bad magic number");
    (void) close(fp->s);
    free((char *)fp);
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
#ifdef __Lynx__
      ((RFILE *)fp)->eof |= _ERR;
#else
      ((RFILE *)fp)->eof |= _IOERR;
#endif
#endif
      break;
  default:
    rc = 0;
#ifdef linux
    ((RFILE *)fp)->eof |= _IO_EOF_SEEN; 
#else
#ifdef __Lynx__
    ((RFILE *)fp)->eof |= _EOF; 
#else
    ((RFILE *)fp)->eof |= _IOEOF; 
#endif
#endif
    break; 
  }
  END_TRACE();
  return rc; 
}
