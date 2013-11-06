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
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <stdlib.h>
#include <string.h>

extern int switch_write();

int
rfio_xywrite(int lun,
	     char* buf,
	     int nrec,
	     int nwrit,
	     char* chopt,
	     int* irc)
{
  char    buffer[128];            /* general purpose buffer       */
  register char *p=buf;           /* buffer pointer               */
  int     optval;                 /* setsockopt() value           */
  int     status;                 /* Fortran status               */
  int     rcode;                  /* Remote return code           */
  int  acc;
  register int i;                 /* general purpose index        */

  TRACE(1, "rfio", "rfio_xywrite(%d, %x, %d, %d, %s, %x)",
        lun, buf, nrec, nwrit, chopt, irc);

  if (ftnlun[lun] == (RFILE *) NULL)      { /* Allocated ?        */
    TRACE(1, "rfio", "rfio_xywrite: %s", "Bad file number");
    END_TRACE();
    return(EBADF);
  }

  TRACE(2, "rfio", "rfio_xywrite: parsing options: [%s]",chopt);
  for (i=0;i< (int)strlen(chopt);i++)   {
    switch (chopt[i])       {
    case ' ': break;
    default :
      *irc = SEBADFOPT;
      END_TRACE();
      return(SEBADFOPT);
    }
  }

  if (!strcmp(ftnlun[lun]->host, "localhost"))      { /* Local file ?   */
    acc = (int) ftnlun[lun]->access;
    *irc = switch_write(acc, &lun, buf, &nwrit, &nrec, LLM);
    END_TRACE();
    return(*irc);
  }

  p = buffer;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_XYWRIT);
  marshall_LONG(p, nrec);
  marshall_LONG(p, nwrit);
  TRACE(2,"rfio","rfio_xywrite: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(ftnlun[lun]->s,buffer,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio" ,"rfio_xywrite: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    if ( serrno )
      return ( serrno ) ;
    else
      return(errno);
  }
  if (ftnlun[lun]->bufsize < nwrit)     {
    optval = nwrit;
    TRACE(2, "rfio", "rfio_xywrite: setsockopt(SOL_SOCKET, SO_SNDBUF): %d", optval);
    ftnlun[lun]->bufsize = nwrit;
    if (setsockopt(ftnlun[lun]->s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)    {
      TRACE(2, "rfio" ,"rfio_xywrite: setsockopt(SO_SNDBUF): ERROR");
    }
  }
  p = buffer;
  TRACE(2, "rfio", "rfio_xywrite: writing %d bytes", nwrit);
  if (netwrite_timeout(ftnlun[lun]->s, buf, nwrit, RFIO_DATA_TIMEOUT) != nwrit)       {
    TRACE(2, "rfio" ,"rfio_xywrite: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    if ( serrno )
      return ( serrno ) ;
    else
      return(errno);
  }
  TRACE(2, "rfio", "rfio_xywrite: reading %d bytes", 2*LONGSIZE);
  if (netread_timeout(ftnlun[lun]->s, buffer, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
    TRACE(2, "rfio" ,"rfio_xywrite: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    if ( serrno )
      return ( serrno );
    else
      return(errno);
  }
  p = buffer;
  unmarshall_LONG(p, status);
  unmarshall_LONG(p, rcode);
  TRACE(1, "rfio", "rfio_xywrite: status %d, rcode %d", status, rcode);
  if ( rcode > SEBASEOFF )
    serrno = rcode ;
  else
    rfio_errno = rcode;
  return(*irc = status);
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
  char    *chopt;         /* xywrite options                      */
  int     status;         /* xywrite return status                */

  INIT_TRACE("RFIO_TRACE");
  if ((chopt = malloc((unsigned) fchoptl+1)) == NULL)        {
    *firc = -errno;
    END_TRACE();
    return;
  }

  strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';

  TRACE(1, "rfio", "XYWRIT(%d, %x, %d, %d, %s, %x)",
        *flun, fbuf, *fnrec, *fnwrit, chopt, firc);
  status = rfio_xywrite(*flun, fbuf, *fnrec, *fnwrit, chopt, firc);
  if (status) *firc = -status;    /* system errors have precedence */
  TRACE(1, "rfio", "XYWRIT: status: %d, irc: %d",status,*firc);
  END_TRACE();
  (void) free(chopt);
  return;
}
