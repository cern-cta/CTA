/*
 * $Id: fchown.c,v 1.5 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/* fchown.c      Remote File I/O - change ownership of a file            */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file chown
 */
int rfio_fchown(int      s,
                int owner,            /* Owner's uid */
                int group)            /* Owner's gid */
{
  int status ;
  int s_index = -1;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_fchown(%d, %d, %d)", s, owner, group);
  /*
   * The file is local
   */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
    TRACE(2, "rfio", "rfio_fchown: using local fchown(%d, %d, %d)", s, owner, group);
    status = fchown(s, owner, group);
    if ( status < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE();
    return(status);
  }

  // Remote directory. Not supported anymore
  rfio_errno = SEOPNOTSUP;
  TRACE(1,"rfio","rfio_fchown: rcode %d", SEOPNOTSUP) ;
  END_TRACE();
  return -1;
}
