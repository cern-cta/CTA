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
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>
#include <string.h>
#include <rfio_xy.h>

extern int DLL_DECL switch_close();

int DLL_DECL rfio_xyclose(lun, chopt, irc)   /* close a remote fortran logical unit  */
     int     lun;
     char    *chopt;
     int     *irc;
{
  char    buf[LONGSIZE];  /* network command/response buffer      */
  register char *p=buf;   /* buffer pointer                       */
  int     status;         /* Fortran status                       */
  register int i;         /* general purpose index                */

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio" ,"rfio_xyclose(%d, %s, %x)", lun,  chopt, irc);
  if (ftnlun[lun] == (RFILE *) NULL)      { /* Allocated ?        */
    TRACE(1, "rfio", "rfio_xyclose: %s", "Bad file number");
    END_TRACE();
    return(EBADF);
  }

  /*
   * Analyze options
   */

  TRACE(2, "rfio", "rfio_xyclose: parsing options: [%s]",chopt);
  for (i=0;i< (int)strlen(chopt);i++)   {
    switch (chopt[i])       {
    case ' ': break;
    default :
      *irc = SEBADFOPT;
      END_TRACE();
      return(SEBADFOPT);
    }
  }


  *irc = 0;

  if (!strcmp(ftnlun[lun]->host, "localhost"))      { /* Local file ?   */
    /* Must be a local file */
    *irc=switch_close(&lun);
    (void) free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
    END_TRACE();
    rfio_errno = 0;
    return(*irc);
  }

  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_XYCLOS);
  TRACE(2,"rfio","rfio_xyclose: writing %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(ftnlun[lun]->s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio" ,"rfio_xyclose: write(): ERROR occured (errno=%d)", errno);
    free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
    END_TRACE();
    if ( serrno )
      return ( serrno ) ;
    else
      return(errno);
  }
  p = buf;
  TRACE(2, "rfio" ,"rfio_xyclose: reading %d bytes", LONGSIZE);
  if (netread_timeout(ftnlun[lun]->s, buf, LONGSIZE, RFIO_CTRL_TIMEOUT) != LONGSIZE) {
    TRACE(2, "rfio" ,"rfio_xyclose: read(): ERROR occured (errno=%d)", errno);
    free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
    END_TRACE();
    if ( serrno )
      return ( serrno ) ;
    else
      return( errno );
  }
  unmarshall_LONG(p, status);
  TRACE(1, "rfio", "rfio_xyclose: return %d ",status);
  END_TRACE();
  (void) close(ftnlun[lun]->s);
  free((char *)ftnlun[lun]); ftnlun[lun]=(RFILE *) NULL;
  *irc = status;
  rfio_errno = status ;
  return(status);
}

/*
 * Fortran wrapper
 */

          void xyclos_(flun, fchopt, firc, fchoptl)
          int     *flun, *firc;
          char    *fchopt;
          int     fchoptl;
{
  char    *chopt;         /* xyclos options                       */
  int     status;         /* xyclos return status                 */

  INIT_TRACE("RFIO_TRACE");
  if ((chopt = malloc((unsigned) fchoptl+1)) == NULL)        {
    *firc = -errno;
    END_TRACE();
    return;
  }

  strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';

  TRACE(1, "rfio", "XYCLOS(%d, %s, %x)", *flun,  chopt, firc);
  status = rfio_xyclose(*flun, chopt, firc);
  if (status) *firc = -status;    /* system errors have precedence */
  TRACE(1, "rfio", "XYCLOS: status: %d, irc: %d",status,*firc);
  END_TRACE();
  (void) free(chopt);
  return;
}
