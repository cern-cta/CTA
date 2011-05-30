/*
 * $Id: close.c,v 1.8 2009/01/09 14:47:39 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2004 by CERN/IT/FIO
 * All rights reserved
 */

/* close.c      Remote File I/O - close a file                          */

#define RFIO_KERNEL     1
#include "Castor_limits.h"
#include "rfio.h"
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */


// Forward declarations
int rfio_close_v2(int s);

/*
 * remote file close
 */
int rfio_close(int     s)
{
  int      s_index;
  int rc;

  /* Remote file ? */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) != -1) {
    if (rfilefdt[s_index]->version3 == 1) {
      /* New V3 stream protocol for sequential transfers */
      rc = rfio_close_v3(s);
    } else {
      rc = rfio_close_v2(s);
    }
  } else {
    rc = rfio_close_v2(s);
  }

  return(rc);
}

int rfio_close_v2(int     s)
{
  char     rfio_buf[BUFSIZ] ;
  char   * p  ;
  int status, status1;
  char  * trp ;  /* Pointer to a temporary buffer  */
  int temp= 0 ;  /* A temporary buffer has been allocated */
  int s_index;

  // Avoiding Valgrind error messages about uninitialized data
  memset(rfio_buf, 0, BUFSIZ);

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_close(%d)", s);

  /*
   * The file is local
   */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {

    TRACE(2, "rfio", "rfio_close: using local close(%d)",s) ;
    status= close(s) ;
    if ( status < 0 ) serrno = 0;
    END_TRACE() ;
    rfio_errno = 0;
    return (status ? status : 0) ;
  }
  /*
   * Checking magic number
   */
  if ( rfilefdt[s_index]->magic != RFIO_MAGIC ) {
    serrno = SEBADVERSION ;
    rfio_rfilefdt_freeentry(s_index);
    (void) close(s) ;
    END_TRACE();
    return(-1);
  }
  /*
   * Sending request.
   */
  memset(rfio_buf, 0, BUFSIZ);
  p= rfio_buf ;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_CLOSE);
  TRACE(2, "rfio", "rfio_close: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(s, rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio", "rfio_close: write(): ERROR occured (errno=%d)", errno);
    (void) rfio_cleanup(s) ;
    END_TRACE() ;
    return -1 ;
  }
  /*
   * Getting data from the network.
   */
  for(;;) {
    WORD    req ;
    LONG  rcode ;
    LONG msgsiz ;

    TRACE(2, "rfio", "rfio_close: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ;
    if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != (ssize_t)rfilefdt[s_index]->_iobuf.hsize) {
      TRACE(2, "rfio", "rfio_close: read(): ERROR occured (errno=%d)", errno);
      if ( temp ) (void) free(trp) ;
      (void)rfio_cleanup(s) ;
      END_TRACE() ;
      return -1 ;
    }
    p = rfio_buf ;
    unmarshall_WORD(p,req) ;
    unmarshall_LONG(p,status) ;
    unmarshall_LONG(p, rcode) ;
    unmarshall_LONG(p,msgsiz) ;
    rfio_errno = rcode ;
    switch(req) {
    case RQST_CLOSE:
      if ( temp ) (void) free(trp) ;
      status1 = rfio_cleanup(s) ;
      TRACE(1, "rfio", "rfio_close: return status=%d, rcode=%d",status,rcode) ;
      END_TRACE() ;
      return (status ? status : status1) ;
    case RQST_READAHEAD:
    case RQST_READAHD64:
    case RQST_LASTSEEK:
    case RQST_PRESEEK:
    case RQST_PRESEEK64:
      /*
       * At this point a temporary buffer may need to be created
       * to receive data which is going to be thrown away.
       */
      if ( temp == 0 ) {
        if ( rfilefdt[s_index]->_iobuf.base==NULL || (int)rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
          temp= 1 ;
          TRACE(3,"rfio","rfio_close: allocating momentary buffer of size %d",msgsiz) ;
          if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
            TRACE(3,"rfio","rfio_close: malloc(): ERROR occured (errno=%d)",errno) ;
            (void) rfio_cleanup(s) ;
            END_TRACE() ;
            return -1 ;
          }
        }
        else
          trp= iodata(rfilefdt[s_index]) ;
      }
      if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
        TRACE(2,"rfio","rfio_close: read(): ERROR occured (errno=%d)",errno) ;
        if ( temp ) (void) free(trp) ;
        (void) rfio_cleanup(s) ;
        END_TRACE() ;
        return -1 ;
      }
      break ;
    default:
      TRACE(1,"rfio","rfio_close(): Bad control word received\n") ;
      serrno= SEINTERNAL ;
      if ( temp ) (void) free(trp) ;
      (void) rfio_cleanup(s) ;
      END_TRACE() ;
      return -1 ;
    }
  }
}
