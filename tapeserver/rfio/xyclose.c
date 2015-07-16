/*
 * $Id: xyclose.c,v 1.7 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* xyclose.c    Remote File I/O - Close a Fortran Logical Unit          */

/*
 * C bindings :
 *
 * rfio_xyclose(int lun, char *chopt, int *irc)
 *
 * FORTRAN bindings :
 *
 * XYCLOS(INTEGER*4 LUN, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 *
 * Note that the FORTRAN API is not supported anymore
 * Any call to FORTRAN API will now result in raising an abort signal 
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>
#include <string.h>
#include <rfio_xy.h>
#include <sys/types.h>
#include <signal.h>

/* close a remote fortran logical unit  */
int rfio_xyclose(int     lun,
                 char    *chopt,
                 int     *irc)
{
  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio" ,"rfio_xyclose(%d, %s, %x)", lun,  chopt, irc);
  TRACE(1,"rfio","rfio_xyclose will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return -1;
}

/*
 * Fortran wrapper
 */
void xyclos_(int     *flun,
             char    *fchopt,
             int  *firc,
             int     fchoptl)
{
  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "XYCLOS(%d, %d, %x)", *flun, firc);
  TRACE(1,"rfio","xyclos will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return;
}
