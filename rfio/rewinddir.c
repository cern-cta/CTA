/*
 * Copyright (C) 1990-1997 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rewinddir.c	1.2 06/02/98 O.Barring";
#endif /* not lint */

/* rewinddir.c      Remote File I/O - rewind a directory                     */
#if !defined(_WIN32)

#define RFIO_KERNEL     1 
#include "rfio.h"        

/*
 * remote directory close
 */

int rfio_rewinddir(dirp)
     RDIR *dirp;
{
  WORD    req ; 
  LONG  rcode ;
  LONG msgsiz ;
  static int s;
  int status;
  char *p;
  extern RDIR *rdirfdt[MAXRFD];

  /*
   * Search internal table for this directory pointer
   * Check first that it's not the last one used
   */
  if ( s<0 || s>=MAXRFD || rdirfdt[s] != dirp )
    for ( s=0; s<MAXRFD; s++ ) if ( rdirfdt[s] == dirp ) break;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_rewinddir(0x%x)", dirp);

  /*
   * The directory is local
   */
  if ( s < 0 || s >= MAXRFD || rdirfdt[s] == NULL ) {
    TRACE(2, "rfio", "rfio_rewinddir: using local rewinddir(0x%x)",dirp) ; 
    (void)rewinddir((DIR *)dirp) ; 
    END_TRACE() ; 
    return(0) ;
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
  marshall_WORD(p, RQST_REWINDDIR);
  TRACE(2, "rfio", "rfio_rewinddir: sending %d bytes",RQSTSIZE) ;
  if (netwrite(s, rfio_buf,RQSTSIZE) != RQSTSIZE) {
    TRACE(2, "rfio", "rfio_rewinddir: write(): ERROR occured (errno=%d)", errno);
    (void) rfio_dircleanup(s) ;
    END_TRACE() ;
    return -1 ;
  }
  /*
   * Getting data from the network.
   */

  TRACE(2, "rfio", "rfio_rewinddir: reading %d bytes",WORDSIZE+3*LONGSIZE) ; 
  if (netread(s,rfio_buf,WORDSIZE+3*LONGSIZE) != WORDSIZE+3*LONGSIZE) {
    TRACE(2, "rfio", "rfio_rewinddir: read(): ERROR occured (errno=%d)", errno);
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
  dirp->offset = 0;
  dirp->dp.dd_loc = 0;
  TRACE(1, "rfio", "rfio_rewinddir: return status=%d, rcode=%d",status,rcode) ;
  END_TRACE() ; 
  return status ;
}
#endif /* _WIN32 */
