/*
 * $Id: pclose.c,v 1.14 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1993-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* pclose.c      Remote command I/O - close a popened command   */

#include <sys/wait.h>
#include <errno.h>
#include <string.h>
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

/*
 * remote pclose
 */
int rfio_pclose(fs)
     RFILE  *fs ;
{
  char   * p  ;
  int  status, fss ;
  char     buf[256];       /* General input/output buffer          */

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_pclose(%x)", fs);

  /*
   * The file is local
   */
  if (rfio_rfilefdt_findptr(fs,FINDRFILE_WITH_SCAN) == -1 ) {
    TRACE(2, "rfio", "rfio_pclose: using local pclose") ;
    status= pclose(fs->fp_save);
    if ( status < 0 ) serrno = 0;
    free((char *)fs);
    END_TRACE() ;
    rfio_errno = 0;
    return status ;
  }

  /*
   * Checking magic number
   */
  if ( fs->magic != RFIO_MAGIC ) {
    serrno = SEBADVERSION ;
    free((char *)fs);
    END_TRACE();
    return -1;
  }

  /*
   * Sending request.
   */
  p= buf ;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_PCLOSE);
  TRACE(2, "rfio", "rfio_pclose: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(fs->s, buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    fss = fs->s;
    TRACE(2, "rfio", "rfio_pclose: write(): ERROR occured (errno=%d)", errno);
    (void) free((char *)fs) ;
    (void)close(fss) ;
    END_TRACE() ;
    return -1 ;
  }
  /*
   * Getting data from the network.
   */
  p = buf ;
  if ( netread_timeout( fs->s , buf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != 2*LONGSIZE)  {
    fss = fs->s;
    TRACE(2,"rfio", "pclose: write(): %s", strerror(errno));
    (void) free((char *)fs) ;
    (void)close(fss) ;
    END_TRACE() ;
    return -1 ;
  }
  unmarshall_LONG(p, status) ;
  unmarshall_LONG(p, rfio_errno) ;
  TRACE(3,"rfio", "rfio_pclose: status is %d, rfio_errno is %d",status,rfio_errno);
  /*
   * freeing RFILE pointer
   */
  fss = fs->s;
  (void) free((char *)fs) ;
  (void) close(fss) ;
  return status ;
}
