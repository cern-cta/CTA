/*
 * $Id: lockf.c,v 1.10 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* lockf.c       Remote File I/O - record locking on files  */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */
#include "rfio_rfilefdt.h"


/* Remote lockf               */
int rfio_lockf(int  sd,  /* file descriptor    */
               int op,  /* lock operation              */
               long  siz)  /* locked region   */
{
  static char     buf[256];       /* General input/output buffer          */
  int             status;         /* remote lockf() status        */
  int      len;
  char     *p=buf;
  int   rcode ;
  int          s_index;


  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_lockf(%d, %d, %ld)", sd, op, siz);

  /*
   * The file is local
   */
  if ((s_index=rfio_rfilefdt_findentry(sd, FINDRFILE_WITHOUT_SCAN)) == -1) {
    TRACE(1, "rfio", "rfio_lockf: using local lockf(%d, %d, %ld)",
          sd, op, siz);
    END_TRACE();
    rfio_errno = 0;
    status = lockf(sd,op,siz);
    if ( status < 0 ) serrno = 0;
    return(status);
  }

  /*
   * Checking mode 64.
   */
  if (rfilefdt[s_index]->mode64) {
    off64_t siz64;

    siz64 = siz;
    status = rfio_lockf64(sd, op, siz64);
    END_TRACE();
    return(status);
  }

  len = 2*LONGSIZE;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_LOCKF);
  marshall_WORD(p, geteuid());
  marshall_WORD(p, getegid());
  marshall_LONG(p, len);
  p= buf + RQSTSIZE;
  marshall_LONG(p, op);
  marshall_LONG(p, siz);
  TRACE(1,"rfio","rfio_lockf: op %d, siz %ld", op, siz);
  TRACE(2,"rfio","rfio_lockf: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite_timeout(sd,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
    TRACE(2, "rfio", "rfio_lockf: write(): ERROR occurred (errno=%d)", errno);
    (void) close(sd);
    END_TRACE();
    return(-1);
  }
  p = buf;
  TRACE(2, "rfio", "rfio_lockf: reading %d bytes", 2*LONGSIZE);
  if (netread_timeout(sd, buf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
    TRACE(2, "rfio", "rfio_lockf: read(): ERROR occurred (errno=%d)", errno);
    (void) close(sd);
    END_TRACE();
    return(-1);
  }
  unmarshall_LONG(p, status);
  unmarshall_LONG(p, rcode);
  TRACE(1, "rfio", "rfio_lockf: return %d",status);
  rfio_errno = rcode;
  if (status)     {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return (0);
}
