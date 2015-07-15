/*
 * $Id: rmdir.c,v 1.14 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rmdir.c       Remote File I/O - make a directory file                */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include <string.h>
#include <unistd.h>


/* Remote rmdir             */
int  rfio_rmdir(char  *dirpath)          /* remote directory path             */
{
  char  *host, *filename;
  int   parserc ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_rmdir(%s)", dirpath);

  if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      TRACE(1,"rfio","rfio_rmdir: %s is an HSM path %s",
            dirpath,host);
      END_TRACE();
      rfio_errno = 0;
      /* This check is needed to support relative directory pathes
       * See the implementation of rfio_HsmIf_rmdir and of the
       * rfio_HsmIf_IsCnsFile method for details. In brief, if
       * a relative directory path is given and rfchdir was used
       * before that, rmdir will be able to work. If no chdir was
       * called, rmdir will fail
       */
      if (filename && (strstr(filename,"/castor")==filename))
        return rfio_HsmIf_rmdir(filename);
      return(rfio_HsmIf_rmdir(dirpath));
    }

    TRACE(1, "rfio", "rfio_rmdir: using local rmdir(%s)",
          filename);

    END_TRACE();
    rfio_errno = 0;
    return(rmdir(filename));
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(1, "rfio", "rfio_rmdir: return %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
