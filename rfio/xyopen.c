/*
 * $Id: xyopen.c,v 1.14 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* xyopen.c     Remote File I/O - Open a Fortran Logical Unit           */

/*
 * C bindings :
 *
 * rfio_xyopen(char *filename, char *host, int lun, int lrecl,
 *            char *chopt, int *irc)
 *
 * FORTRAN bindings :
 *
 * XYOPEN(INTEGER*4 LUN, INTEGER*4 LRECL, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 * XYOPN(CHARACTER*(*) FILENAME, CHARACTER*(*)HOST, INTEGER*4 LUN,
 *       INTEGER*4 LRECL, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 *
 * Note that the FORTRAN API is not supported anymore
 * Any call to FORTRAN API will now result in raising an abort signal 
 */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include <sys/param.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <pwd.h>
#include <Cpwd.h>
#include <rfio_xy.h>
#include <sys/types.h>
#include <signal.h>

RFILE *ftnlun[MAXFTNLUN];       /* Fortran logical units       */

int rfio_xysock(int lun)
{
  /*
   * Not supported any more -> we raise an abort signal
   */
  kill(0, SIGABRT);
  return -1 ;
}

int rfio_xyopen(char    *name,
                char *node,
                int     lun,
                int lrecl,
                char *chopt,
                int     *irc)
{
  /*
   * Not supported any more -> we raise an abort signal
   */
  INIT_TRACE("RFIO_TRACE");       /* initialize trace if any      */
  TRACE(1,"rfio","rfio_xyopen(%d,%d,?,%d)",lun,lrecl,*irc);
  TRACE(1,"rfio","rfio_xyopen will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return -1;
}  

void xyopen_(int     *flun,
             int *flrecl,
             char    *fchopt,
             int *firc,
             int     fchoptl)
{
  /*
   * Not supported any more -> we raise an abort signal
   */
  INIT_TRACE("RFIO_TRACE");       /* initialize trace if any      */
  TRACE(1,"rfio","XYOPEN(%d,%d,?,%d)",*flun,*flrecl,*firc);
  TRACE(1,"rfio","xyopen will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return;
}

void xyopn_(char    *fname,
            char  *fnode,
            int     *flun,
            int *flrecl,
            char *fchopt,
            int *firc,
            int     fnamel,
            int fnodel,
            int fchoptl)
{
  INIT_TRACE("RFIO_TRACE");       /* initialize trace if any      */
  TRACE(1,"rfio","XYOPN(?, ?, %d, %d, ?, %d)", *flun,*flrecl,*firc);
  TRACE(1,"rfio","xyopn will raise abort signal");
  END_TRACE();
  kill(0, SIGABRT);
  return;
}
