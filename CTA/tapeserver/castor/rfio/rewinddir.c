/*
 * $Id: rewinddir.c,v 1.15 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rewinddir.c      Remote File I/O - rewind a directory                     */

#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rdirfdt.h"

/*
 * remote directory close
 */

int rfio_rewinddir(RDIR *dirp)
{
  int s_index;

  /*
   * Search internal table for this directory pointer
   * Check first that it's not the last one used
   */
  s_index = rfio_rdirfdt_findptr(dirp,FINDRDIR_WITH_SCAN);

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_rewinddir(0x%x)", dirp);

  /*
   * The directory is local
   */
  if (s_index == -1) {
    TRACE(2, "rfio", "rfio_rewinddir: check if HSM directory");
    if ( rfio_HsmIf_IsHsmDirEntry((DIR *)dirp) != -1 ) {
      (void)rfio_HsmIf_rewinddir((DIR *)dirp);
    } else {
      TRACE(2, "rfio", "rfio_rewinddir: using local rewinddir(0x%x)",dirp) ;
      (void)rewinddir((DIR *)dirp) ;
    }
    END_TRACE();
    return(0) ;
  }

  // Remote directory. Not supported anymore
  rfio_errno = SEOPNOTSUP ;
  TRACE(1, "rfio", "rfio_rewinddir: rcode=%d", SEOPNOTSUP) ;
  END_TRACE() ;
  return -1 ;
}
