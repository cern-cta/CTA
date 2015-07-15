/*
 * $Id: fstat.c,v 1.18 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* fstat.c      Remote File I/O - get file status                       */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file stat
 */
int rfio_fstat(int s, struct stat *statbuf) {
  int status ;
  struct stat64 statb64;
  if ((status = rfio_fstat64(s,&statb64)) == 0)
    (void) stat64tostat(&statb64, statbuf);
  return (status);
}
