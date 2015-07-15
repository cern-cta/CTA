/*
 * $Id: chown.c,v 1.12 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* chown.c       Remote File I/O - Change a file owner */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include <string.h>
#include <unistd.h>

/* Remote chown                 */
int rfio_chown(char  *file,          /* remote file path             */
               int  owner,     /* Owner's uid */
               int   group)     /* Owner's gid */
{
  int             status;         /* remote chown() status        */
  char     *host,
    *filename;
  int parserc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_chown(%s, %d, %d)", file,owner,group);

  if (!(parserc = rfio_parseln(file,&host,&filename,NORDLINKS))) {
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_chown: %s is an HSM path",
            filename);
      END_TRACE();
      rfio_errno = 0;
      return(rfio_HsmIf_chown(filename,owner,group));
    }
    TRACE(1, "rfio", "rfio_chown: using local chown(%s, %d, %d)",
          filename, owner, group);

    END_TRACE();
    rfio_errno = 0;
    status = chown(filename,owner, group);
    if ( status < 0 ) serrno = 0;
    return(status);
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(1, "rfio", "rfio_chown: return %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
