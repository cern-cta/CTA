/*
 * $Id: closedir.c,v 1.14 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* closedir.c      Remote File I/O - close a directory                     */

#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rdirfdt.h"

/*
 * remote directory close
 */

int rfio_closedir(RDIR *dirp)
{
  int status;
  int s_index;

  /*
   * Search internal table for this directory pointer
   */
  s_index = rfio_rdirfdt_findptr(dirp,FINDRDIR_WITH_SCAN);

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_closedir(0x%x)", dirp);

  /*
   * The directory is local
   */
  if (s_index == -1) {
    TRACE(2, "rfio", "rfio_closedir: check if HSM directory");
    if ( rfio_HsmIf_IsHsmDirEntry((DIR *)dirp) != -1 ) {
      status = rfio_HsmIf_closedir((DIR *)dirp);
    } else {
      TRACE(2, "rfio", "rfio_closedir: using local closedir(0x%x)",dirp) ;
      status= closedir((DIR *)dirp) ;
      if ( status < 0 ) serrno = 0;
    }
    END_TRACE() ;
    return status ;
  }

  // Remote directory. Not supported anymore
  TRACE(1, "rfio", "rfio_closedir: rcode=%d",SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return -1;
}
