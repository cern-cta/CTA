/*
 * $Id: closedir.c,v 1.2 1999/07/20 12:47:54 jdurand Exp $
 *
 * $Log: closedir.c,v $
 * Revision 1.2  1999/07/20 12:47:54  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1997 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)closedir.c	1.2 02 Jun 1998 O.Barring";
#endif /* not lint */

/* closedir.c      Remote File I/O - close a directory                     */

#if !defined(_WIN32)
#define RFIO_KERNEL     1 
#include "rfio.h"        

/*
 * remote directory close
 */

int rfio_closedir(dirp)
     RDIR *dirp;
{
  WORD    req ; 
  LONG  rcode ;
  LONG msgsiz ;
  int s;
  int status;
  char *p;
  extern RDIR *rdirfdt[MAXRFD];

  /*
   * Search internal table for this directory pointer
   */
  for ( s=0; s<MAXRFD; s++ ) if ( rdirfdt[s] == dirp ) break;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_closedir(0x%x)", dirp);

  /*
   * The directory is local
   */
  if ( s < 0 || s >= MAXRFD || rdirfdt[s] == NULL ) {
    TRACE(2, "rfio", "rfio_closedir: using local closedir(0x%x)",dirp) ; 
    status= closedir((DIR *)dirp) ; 
    END_TRACE() ; 
    return status ;
  }
  /*
   * Checking magic number
   */
  if ( rdirfdt[s]->magic != RFIO_MAGIC ) {
    serrno = SEBADVERSION ;
    (void) close(s) ;
    free((char *)rdirfdt[s]);
    rdirfdt[s] = 0;
    END_TRACE();
    return(-1);
  }
  /*
   * Sending request.
   */
  p= rfio_buf ;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_CLOSEDIR);
  TRACE(2, "rfio", "rfio_closedir: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(s, rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio", "rfio_closedir: write(): ERROR occured (errno=%d)", errno);
    (void) rfio_dircleanup(s) ;
    END_TRACE() ;
    return -1 ;
  }
  /*
   * Getting data from the network.
   */

  TRACE(2, "rfio", "rfio_closedir: reading %d bytes",WORDSIZE+3*LONGSIZE) ; 
  if (netread_timeout(s,rfio_buf,WORDSIZE+3*LONGSIZE,RFIO_CTRL_TIMEOUT) != (WORDSIZE+3*LONGSIZE)) {
    TRACE(2, "rfio", "rfio_closedir: read(): ERROR occured (errno=%d)", errno);
    (void)rfio_dircleanup(s) ; 
    END_TRACE() ;
    return -1 ; 
  }
  p = rfio_buf ;
  unmarshall_WORD(p,req) ;
  unmarshall_LONG(p,status) ;
  unmarshall_LONG(p, rcode) ;
  unmarshall_LONG(p,msgsiz) ;
  rfio_errno = rcode ;
  (void) rfio_dircleanup(s) ; 
  TRACE(1, "rfio", "rfio_closedir: return status=%d, rcode=%d",status,rcode) ;
  END_TRACE() ; 
  return status ;
}
#endif /* _WIN32 */
