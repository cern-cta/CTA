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

RFILE *ftnlun[MAXFTNLUN];       /* Fortran logical units       */

int rfio_xysock(int lun)
{
  return -1 ;
}

int
rfio_xyopen_ext(char *name,
		char *node,
		int lun,
		int lrecl,
		char *chopt,
		int *irc,
		uid_t uid,
		gid_t gid,
		int key,
		char *reqhost)
{
  INIT_TRACE("RFIO_TRACE");
  TRACE(2,"--->","OPTIONS: %s , %s, %d,%d,%s,%d,%d,%d,%d,%s",name,node,lun,lrecl,chopt,*irc,uid,gid,key,reqhost);
  TRACE(1, "rfio", "rfio_xyopen: status: %d", SEOPNOTSUP);
  END_TRACE();
  return SEOPNOTSUP;
}

int rfio_xyopen(char    *name,
                char *node,
                int     lun,
                int lrecl,
                char *chopt,
                int     *irc)
{
  char rh[1];
  rh[0]='\0';
  return( rfio_xyopen_ext(name,node,lun,lrecl,chopt,irc,(uid_t)0,(gid_t)0,0,rh) );
}
