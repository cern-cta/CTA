/*
 * $Id: chmod.c,v 1.12 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* chmod.c       Remote File I/O - change file mode                     */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include <string.h>
#include <unistd.h>

/* Remote chmod         */
int rfio_chmod(char  *dirpath,          /* remote directory path             */
	       int  mode)              /* remote directory mode             */
{
  int             status;         /* remote chmod() status        */
  char     *host,
    *filename;
  int         parserc;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_chmod(%s, %o)", dirpath, mode);

  if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_chmod: %s is an HSM path",
            filename);
      END_TRACE();
      rfio_errno = 0;
      return(rfio_HsmIf_chmod(filename,mode));
    }
    TRACE(1, "rfio", "rfio_chmod: using local chmod(%s, %o)",
          filename, mode);

    END_TRACE();
    rfio_errno = 0;
    status = chmod(filename,mode);
    if ( status < 0 ) serrno = 0;
    return(status);
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(1, "rfio", "rfio_chmod: return %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
