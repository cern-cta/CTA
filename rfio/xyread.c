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

extern int switch_read();

int
rfio_xyread(lun, buf, nrec, nwant, ngot, chopt, irc)
     int     lun, nrec, nwant;
     char    *buf, *chopt;
     int     *ngot, *irc;
{
  char    buffer[128];            /* general purpose buffer       */
  register char *p=buf;           /* buffer pointer               */
  register int     readopt;       /* read options                 */
  int     optval;                 /* setsockopt() value           */
  register int     rbufsiz = 0;   /* read buffer size             */
  int     status;                 /* Fortran status               */
  int     rcode;                  /* Remote return code           */
  register int i;                 /* general purpose index        */
  int  acc;

  TRACE(1, "rfio", "rfio_xyread(%d, %x, %d, %d, %x, %s, %x)",
        lun, buf, nrec, nwant, ngot, chopt, irc);

  if (ftnlun[lun] == (RFILE *) NULL)      { /* Allocated ?        */
    TRACE(1, "rfio", "rfio_xyread: %s", "Bad file number");
    END_TRACE();
    return(EBADF);
  }

  TRACE(2, "rfio", "rfio_xyread: parsing options: [%s]",chopt);
  readopt = FFREAD_N;
  for (i=0;i< (int)strlen(chopt);i++)   {
    switch (chopt[i])       {
    case 'u':
    case 'U':
      readopt = FFREAD_C;
      break;
    case ' ': break;
    default :
      *irc = SEBADFOPT;
      END_TRACE();
      return(SEBADFOPT);
    }
  }

  if (!strcmp(ftnlun[lun]->host, "localhost"))      { /* Local file ?   */
    acc = (int)ftnlun[lun]->access;
    *irc=switch_read(acc,&lun,buf,&nwant, &nrec, readopt, ngot, LLM );
    END_TRACE();
    return(*irc);
  }

  p = buffer;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_XYREAD);
  marshall_LONG(p, readopt);
  marshall_LONG(p, nrec);
  marshall_LONG(p, nwant);
  TRACE(2,"rfio","rfio_xyread: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(ftnlun[lun]->s,buffer,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio" ,"rfio_xyread: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    if ( serrno )
      return ( serrno ) ;
    else
      return(errno);
  }
  if (rbufsiz < nwant)     {
    optval = nwant;
    TRACE(2, "rfio", "rfio_xyread: setsockopt(SOL_SOCKET, SO_RCVBUF): %d", optval);
    if (setsockopt(ftnlun[lun]->s, SOL_SOCKET, SO_RCVBUF, (char *)&optval, sizeof(optval)) == -1)    {
      TRACE(2, "rfio" ,"rfio_xyread: setsockopt(SO_RCVBUF): ERROR");
    }
  }
  TRACE(2,"rfio","rfio_xyread: reading %d bytes",3*LONGSIZE) ;
  if (netread_timeout(ftnlun[lun]->s, buffer, 3*LONGSIZE, RFIO_CTRL_TIMEOUT) != (3*LONGSIZE)) {
    TRACE(2,"rfio","rfio_xyread: read(): ERROR occured (errno=%d)",errno) ;
    END_TRACE();
    if ( serrno )
      return (serrno) ;
    else
      return(errno);
  }
  p = buffer;
  unmarshall_LONG(p, status);
  unmarshall_LONG(p, rcode);
  unmarshall_LONG(p, *ngot);
  if ( *ngot ) {
    TRACE(2, "rfio", "rfio_xyread: reading %d bytes", *ngot);
    if (netread_timeout(ftnlun[lun]->s, buf, *ngot, RFIO_DATA_TIMEOUT) != *ngot)       {
      TRACE(2, "rfio" ,"rfio_xyread: read(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      if ( serrno )
        return ( serrno ) ;
      else
        return(errno);
    }
  }
  TRACE(1, "rfio", "rfio_xyread: status %d, rcode %d", status, rcode) ;
  if ( rcode > SEBASEOFF )
    serrno = rcode ;
  else
    rfio_errno= rcode;
  return(*irc = status);
}

/*
 * Fortran wrapper
 */

void xyread_(int     *flun,
             char    *fbuf,
             int *fnrec,
             int *fnwant,
             int *fngot,
             char *fchopt,
             int *firc,
             int     fchoptl)
{
  char    *chopt;         /* xyread options                       */
  int     status;         /* xyread return status                 */

  INIT_TRACE("RFIO_TRACE");
  if ((chopt = malloc((unsigned) fchoptl+1)) == NULL)        {
    *firc = -errno;
    END_TRACE();
    return;
  }

  strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';

  TRACE(1, "rfio", "XYREAD(%d, %x, %d, %d, %x, %s, %x)",
        *flun, fbuf, *fnrec, *fnwant, fngot, chopt, firc);
  status = rfio_xyread(*flun, fbuf, *fnrec, *fnwant, fngot, chopt, firc);
  if (status) *firc = -status;    /* system errors have precedence */
  TRACE(1, "rfio", "XYREAD: status: %d, irc: %d",status,*firc);
  END_TRACE();
  (void) free(chopt);
  return;
}
