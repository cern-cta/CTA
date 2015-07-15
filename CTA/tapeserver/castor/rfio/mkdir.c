/*
 * $Id: mkdir.c,v 1.13 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* mkdir.c       Remote File I/O - make a directory file                */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include <string.h>

/* Remote mkdir              */
int rfio_mkdir(char  *dirpath,          /* remote directory path             */
	       int  mode)              /* remote directory mode             */
{
  int             status;         /* remote mkdir() status        */
  char     *host;
  char     *filename;
  int parserc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_mkdir(%s, %o)", dirpath, mode);

  if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_mkdir: %s is an HSM path",
            filename);
      END_TRACE();
      rfio_errno = 0;
      return(rfio_HsmIf_mkdir(filename,mode));
    }
    TRACE(1, "rfio", "rfio_mkdir: using local mkdir(%s, %o)",
          filename, mode);

    END_TRACE();
    rfio_errno = 0;
    status = mkdir(filename,mode);
    if ( status < 0 ) serrno = 0;
    return(status);
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(1, "rfio", "rfio_mkdir: return %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
