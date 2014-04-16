/*
 * $Id: xyread.c,v 1.6 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* xyread.c     Remote File I/O - Read a Fortran Logical Unit           */

/*
 * C bindings :
 *
 * rfio_xyread(int lun, char *buf, int nrec, int nwant, int *ngot
 *             char *chopt, int *irc);
 *
 * FORTRAN bindings :
 *
 * XYREAD(INTEGER*4 LUN, CHARACTER*(*)BUF, INTEGER*4 NREC,
 *        INTEGER*4 NWANT, INTEGER*4 NGOT, CHARACTER*(*)CHOPT,
 *        INTEGER*4 IRC)
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>
#include <string.h>
#include <rfio_xy.h>

int rfio_xyread(int lun,
		char *buf,
		int nrec,
		int nwant,
		int *ngot,
		char *chopt,
		int *irc)
{
  TRACE(1, "rfio", "rfio_xyread(%d, %x, %d, %d, %x, %s, %x)",
        lun, buf, nrec, nwant, ngot, chopt, irc);
  TRACE(1, "rfio", "rfio_xyread: status %d", SEOPNOTSUP) ;
  rfio_errno = SEOPNOTSUP;
  return SEOPNOTSUP;
}
