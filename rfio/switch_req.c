/*
 * $Id: switch_req.c,v 1.8 2009/06/03 13:57:06 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 *
 * This is a file of functions that merge some code common
 * to rfio_fcalls.c and xyopen.c, xyclose.c, xywrite.c and  xyread.c
 *
 */

#define DEBUG           0               /* Debugging flag               */
#define RFIO_KERNEL     1               /* KERNEL part of the programs  */

#include "rfio.h"                       /* Remote file I/O              */
#include "osdep.h"
#include <log.h>                        /* Genralized error logger      */
#include "fio.h"                       /* Remote file I/O              */

int DLL_DECL switch_open(access, lun, filename, filen, lrecl,append,trunc,mod)
     int     *access  ;
     LONG *lun  ;
     char  *filename ;
     int *filen  ;
     LONG  *lrecl  ;
     LONG   *append  ;
     LONG *trunc  ;
     int  mod  ;
{
  (void)filen;
  int status;
  switch ((int)*access) {
  case FFFACC_S:
    if (mod == LLTM)
      log(LOG_INFO, "rxyopen(%s) SEQUENTIAL\n",filename);
    else
      TRACE(2, "rfio",  "rfio_xyopen(%s) SEQUENTIAL (local)",filename);

    status=usf_open(lun, filename, append,trunc);
    break;
  case FFFACC_D:
    if (mod == LLTM)
      log(LOG_INFO, "rxyopen(%s) DIRECT\n",filename);
    else {
      TRACE(2, "rfio",  "rfio_xyopen(%s) DIRECT (local)",filename);
    }

    status=udf_open(lun, filename, lrecl,trunc);
    break;
  default:
    if (mod == LLTM)
      log(LOG_ERR, "rxyopen(%s) invalid access type: %d\n",filename, *access);
    else {
      TRACE(2, "rfio",  "rfio_xyopen(%d) invalid access type: %d\n",lun, *access);
    }
    status = SEBADFOPT;
  }
  return (status);

}

int DLL_DECL switch_write(access,lun,ptr,nwrit,nrec,mod)
     int     access          ;
     LONG    *lun            ;
     char  *ptr  ;
     int  *nwrit  ;
     int *nrec  ;
     int  mod   ;
{
  int status;
  switch (access) {
  case FFFACC_S:
    if (mod == LLTM)
      log(LOG_DEBUG, "rxywrit(%d) SEQUENTIAL\n",*lun);
    else {
      TRACE(2, "rfio",  "rfio_xywrit(%d) SEQUENTIAL",*lun);
    }

    status=usf_write(lun, ptr, nwrit);
    break;
  case FFFACC_D:
    if (mod == LLTM)
      log(LOG_DEBUG, "rxywrit(%d) DIRECT\n",*lun);
    else
      TRACE(2, "rfio",  "rfio_xywrit(%d) DIRECT",*lun);

    status=udf_write(lun, ptr, nrec, nwrit);
    break;
  default:
    if (mod == LLTM)
      log(LOG_ERR, "rxyopen(%d) invalid access type: %d\n",*lun, access);
    else
      TRACE(2, "rfio",  "rfio_xywrite(%d) invalid access type:%d",*lun, access);

    status = SEBADFOPT;
  }
  return (status);
}


int DLL_DECL switch_read(access,ptlun,buffer1,nwant,nrec,readopt,ngot,mod)
     int   access ;
     int  *ptlun   ;
     char  *buffer1;
     int *nwant ;
     int  *nrec ;
     int readopt ;
     int *ngot ;
     int  mod ;

{
  int status;
  if (readopt == FFREAD_C)        {
    if (mod == LLTM)
      log(LOG_DEBUG, "rxyread(%d) SPECIAL\n",*ptlun);
    else {
      TRACE(2, "rfio", "rfio_xyread(%d) SPECIAL",*ptlun);
    }

    (void) uf_cread(ptlun, buffer1, nrec, nwant, ngot, &status);

  }
  else    {
    switch (access) {
    case FFFACC_S:
      if (mod == LLTM)
        log(LOG_DEBUG, "rxyread(%d) SEQUENTIAL\n",*ptlun);
      else
        TRACE(2, "rfio", "rfio_xyread(%d) SEQUENTIAL",*ptlun);
      status=usf_read(ptlun, buffer1, nwant);

      *ngot = *nwant;
      break;
    case FFFACC_D:
      if (mod == LLTM)
        log(LOG_DEBUG, "rxyread(%d) DIRECT\n",*ptlun);
      else
        TRACE(2, "rfio", "rfio_xyread(%d) DIRECT",*ptlun);
      status =udf_read(ptlun, buffer1, nrec, nwant);

      *ngot = *nwant;
      break;
    default:
      if (mod == LLTM)
        log(LOG_ERR, "rxyread(%d) invalid access type: %d\n",*ptlun, access);
      else
        TRACE(2, "rfio", "rfio_xyread(%d) invalid access type: %d",*ptlun, access);
      *ngot = 0;
      status = SEBADFOPT;
    }
  }
  return (status);

}

int DLL_DECL switch_close(lun)
     int *lun;
{
  int irc;
  irc=uf_close(lun);
  return(irc);
}
