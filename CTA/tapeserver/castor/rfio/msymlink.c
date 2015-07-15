/*
 * $Id: msymlink.c,v 1.16 2008/07/31 07:09:13 sponcec3 Exp $
 */


/*
 * Copyright (C) 1995-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdlib.h>
#include "Cmutex.h"
#include "Castor_limits.h"
#include <osdep.h>
#include "log.h"
#define RFIO_KERNEL 1
#include "rfio.h"
#include <Cglobals.h>
#include <Cpwd.h>
#include <net.h>
#include <string.h>
#include <unistd.h>

int rfio_msymlink(char *n1,
                  char *file2)
{
  int rc, Tid, parserc;
  char *host , *filename ;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(1, "rfio", "rfio_msymlink(\"%s\",\"%s\"), Tid=%d", n1, file2, Tid);
  if (!(parserc = rfio_parseln(file2,&host,&filename,NORDLINKS))) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      rfio_errno = 0;
      serrno = SEOPNOTSUP;
      rc = -1;
      END_TRACE();
      return(rc);
    }
    /* The file is local */
    rc = symlink(n1,filename) ;
    if ( rc < 0 ) serrno = 0;
    rfio_errno = 0;
    END_TRACE();
    return (rc) ;
  }
  if (parserc < 0) {
    END_TRACE();
    return(-1);
  }

  // Remote directory. Not supported anymore
  TRACE(3,"rfio","rfio_msymlink: return %d", SEOPNOTSUP);
  rfio_errno = SEOPNOTSUP;
  END_TRACE();
  return(-1);
}
