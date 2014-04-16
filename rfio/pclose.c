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
int rfio_pclose(RFILE  *fs)
{
  int  status;

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

  // Remote call. Not supported anymore
  TRACE(3,"rfio", "rfio_pclose: status is %d, rfio_errno is %d",SEOPNOTSUP,SEOPNOTSUP);
  rfio_errno= SEOPNOTSUP;
  END_TRACE();
  return SEOPNOTSUP ;
}
