/*
 * $Id: fclose.c,v 1.17 2009/01/09 14:47:39 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fclose.c     Remote File I/O - close a binary file                   */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "Castor_limits.h"
#include "rfio.h"
#include "rfio_rfilefdt.h"
#include <stdlib.h>

/* Remote file close           */
int rfio_fclose(RFILE *fp)                      /* Remote file pointer                  */
{
  int     status;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fclose(%x)", fp);

  if ( fp == NULL ) {
    errno = EBADF;
    END_TRACE();
    return -1 ;
  }

  /*
   * Check if file is Hsm. For CASTOR HSM files, the file is
   * closed using normal RFIO (local or remote) close().
   */
  /*
   * The file is local : this is the only way to detect it !
   */
  if (rfio_rfilefdt_findptr(fp,FINDRFILE_WITH_SCAN) == -1) {
    status= fclose((FILE *)fp) ;
    if ( status < 0 ) serrno = 0;
    END_TRACE() ;
    rfio_errno = 0;
    return (status ? status : 0) ;
  }

  /*
   * The file is remote
   */
  if ( fp->magic != RFIO_MAGIC ) {
    int fps = fp->s;

    free((char *)fp);
    (void) close(fps);
    END_TRACE()  ;
    return -1 ;
  }

  status= rfio_close(fp->s) ;
  END_TRACE() ;
  return status ;
}
