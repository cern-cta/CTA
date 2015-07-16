/*
 * $Id: xywrite.c,v 1.6 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* xywrite.c    Remote File I/O - Write a Fortran Logical Unit          */

/*
 * C bindings :
 *
 * rfio_xywrite(int lun, char *buf, int nrec, int nwrit,
 *             char *chopt, int *irc);
 *
 * FORTRAN bindings :
 *
 * XYWRITE(INTEGER*4 LUN, CHARACTER*(*)BUF, INTEGER*4 NREC,
 *         INTEGER*4 NWANT, INTEGER*4 NGOT, CHARACTER*(*)CHOPT,
 *         INTEGER*4 IRC)
 *
 * Note that the FORTRAN API is not supported anymore
 * Any call to FORTRAN API will now result in raising an abort signal 
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>

int
rfio_xywrite(int lun,
	     char* buf,
	     int nrec,
	     int nwrit,
	     char* chopt,
	     int* irc)
{
  TRACE(1, "rfio", "rfio_xywrite(%d, %x, %d, %d, %s, %x)",
        lun, buf, nrec, nwrit, chopt, irc);
  TRACE(1,"rfio","rfio_xywrite will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return -1;
}

/*
 * Fortran wrapper
 */
void xywrit_(int* flun,
	     char* fbuf,
	     int* fnrec,
	     int* fnwrit,
	     char* fchopt,
	     int* firc,
	     int fchoptl)
{
  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "XYWRIT(%d, %x, %d, %d, ?, %x)",
        *flun, fbuf, *fnrec, *fnwrit, firc);
  TRACE(1,"rfio","xywrite will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return;
}
