/*
 * $Id: fseeko64.c,v 1.1 2002/11/19 10:51:23 baud Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fseeko64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:23 $ CERN/IT/PDP/DM Fabien Collin, Philippe Gaillardon";
#endif /* not lint */

/* fseeko64.c      Remote File I/O - fseek library call */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h"    
#include "rfio_rfilefdt.h"
#include "u64subr.h"

/*
 * Remote file fseek
 */
int DLL_DECL rfio_fseeko64(fp, offset, whence)  
     RFILE *fp;
     off64_t offset;
     int whence;
{
  int     rc;
  off64_t offsetout;
  char tmpbuf[21];
  
  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fseeko64(%x, %s, %d)", fp, u64tostr(offset,tmpbuf,0), whence);
  
  /*
   * Checking fp validity
   */
  if (fp == NULL) {
    errno = EBADF;
    TRACE(2,"rfio","rfio_fseeko64() : FILE ptr is NULL ");
    END_TRACE();
    return -1;
  }

  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    TRACE(2,"rfio","rfio_fseeko64() : using local fseeko64() ");
    rc = fseeko64((FILE *)fp, offset, whence);
    if ( rc < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE(); 
    return rc;
  }

  TRACE(2,"rfio","rfio_fseeko64() : after remoteio") ;

  /*
   * Checking magic number
   */
  if (fp->magic != RFIO_MAGIC) {
    int fps = fp->s;
    serrno = SEBADVERSION; 
    TRACE(2, "rfio", "rfio_fseeko64() : Bad magic number");
    free((char *)fp);
    (void) close(fps);
    END_TRACE();
    return -1;
  }

  /*
   * The file is remote 
   */
  offsetout = rfio_lseek64(fp->s, offset, whence);
  if ( offsetout == (off64_t)-1 ) {
    rc = -1;
#ifdef linux
    ((RFILE *)fp)->eof |= _IO_ERR_SEEN;
#else
#ifdef __Lynx__
    ((RFILE *)fp)->eof |= _ERR;
#else
    ((RFILE *)fp)->eof |= _IOERR;
#endif
#endif
  }
  else {
    rc = 0;
  }
  END_TRACE();
  return rc; 
}
