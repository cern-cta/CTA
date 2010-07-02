/*
 * $Id: write.c,v 1.17 2009/01/09 14:47:39 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* write.c      Remote File I/O - write a file                          */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file write
 */
int rfio_write(int     s,
               void    *ptr,
               int  size)
{
  int s_index;

  /* Remote file ? */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) != -1)
  {
    if (rfilefdt[s_index]->version3 == 1)
    {
      /* New V3 stream protocol for sequential transfers */
      return(rfio_write_v3(s,(char *)ptr,size));
    }
    else
      return(rfio_write_v2(s,(char *)ptr,size));
  }
  else
    return(rfio_write_v2(s,(char *)ptr,size));
}

int rfio_write_v2(int     s,
                  char    *ptr,
                  int  size)
{
  int status ; /* Return code of called func */
  char   * p ;  /* Pointer to buffer  */
  char * trp ;  /* Pointer to a temp buffer */
  int temp=0 ;  /* Has it been allocated ? */
  char     rfio_buf[BUFSIZ];
  int s_index;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_write(%d, %x, %d)", s, ptr, size) ;

#if defined (CLIENTLOG)
  /* Client logging */
  rfio_logwr(s,size);
#endif

  /*
   * The file is local.
   */
  if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
    TRACE(2, "rfio", "rfio_write: using local write(%d, %x, %d)", s, ptr, size);
    status = write(s, ptr, size);
    if ( status < 0 ) serrno = 0;
    END_TRACE();
    rfio_errno = 0;
    return(status);
  }

  /*
   * Checking magic number.
   */
  if (rfilefdt[s_index]->magic != RFIO_MAGIC) {
    serrno = SEBADVERSION ;
    rfio_rfilefdt_freeentry(s_index);
    (void) close(s) ;
    END_TRACE();
    return(-1);
  }

  /*
   * Checking mode 64.
   */
  if (rfilefdt[s_index]->mode64) {
    status = rfio_write64_v2(s, ptr, size);
    END_TRACE();
    return(status);
  }

  /*
   * Repositionning file mark if needed.
   */
  if ( (rfilefdt[s_index]->readissued || rfilefdt[s_index]->preseek) && rfilefdt[s_index]->lseekhow == -1 ) {
    rfilefdt[s_index]->lseekhow= SEEK_SET ;
    rfilefdt[s_index]->lseekoff= rfilefdt[s_index]->offset ;
  }
  /*
   * Resetting flags
   */
  rfilefdt[s_index]->eof= 0 ;
  rfilefdt[s_index]->preseek= 0 ;
  rfilefdt[s_index]->nbrecord= 0 ;
  rfilefdt[s_index]->readissued= 0 ;
  if ( rfilefdt[s_index]->_iobuf.base ) {
    rfilefdt[s_index]->_iobuf.count= 0 ;
    rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ;
  }
  /*
   * Sending request.
   */
  p= rfio_buf ;
  marshall_WORD(p, RFIO_MAGIC);
  marshall_WORD(p, RQST_WRITE);
  marshall_LONG(p, size);
  marshall_LONG(p, rfilefdt[s_index]->lseekhow) ;
  marshall_LONG(p, rfilefdt[s_index]->lseekoff) ;
  rfilefdt[s_index]->lseekhow= -1 ;
  TRACE(2, "rfio", "rfio_write: sending %d bytes",RQSTSIZE) ;
  if (netwrite_timeout(s, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2,"rfio","rfio_write: write(): ERROR occured (errno=%d)",errno) ;
    END_TRACE() ;
    return -1 ;
  }
  if (rfilefdt[s_index]->bufsize < size) {
    rfilefdt[s_index]->bufsize = size ;
    TRACE(2, "rfio", "rfio_write: setsockopt(SOL_SOCKET, SO_SNDBUF): %d", rfilefdt[s_index]->bufsize) ;
    if (setsockopt(s,SOL_SOCKET,SO_SNDBUF,(char *)&(rfilefdt[s_index]->bufsize),sizeof((rfilefdt[s_index]->bufsize))) == -1) {
      TRACE(2, "rfio" ,"rfio_write: setsockopt(SO_SNDBUF)") ;
    }
  }
  TRACE(2,"rfio","rfio_write: sending %d bytes",size) ;
  if (netwrite_timeout(s, ptr, size, RFIO_DATA_TIMEOUT) != size) {
    TRACE(2,"rfio","rfio_write: write(): ERROR occured (errno=%d)",errno) ;
    END_TRACE() ;
    return -1 ;
  }
  /*
   * Getting request answer.
   */
  for(;;) {
    WORD    req ;
    LONG  rcode ;
    LONG msgsiz ;

    TRACE(2, "rfio", "rfio_write: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ;
    if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_CTRL_TIMEOUT) != (ssize_t)rfilefdt[s_index]->_iobuf.hsize) {
      TRACE(2, "rfio", "rfio_write: read(): ERROR occured (errno=%d)", errno);
      if ( temp ) (void) free(trp) ;
      END_TRACE() ;
      return -1 ;
    }
    p = rfio_buf ;
    unmarshall_WORD(p,req) ;
    unmarshall_LONG(p,status) ;
    unmarshall_LONG(p,rcode) ;
    unmarshall_LONG(p,msgsiz) ;
    switch(req) {
    case RQST_WRITE:
      rfio_errno = rcode;
      if ( status < 0 ) rfio_HsmIf_IOError(s,rfio_errno);
      if ( status < 0 && rcode == 0 )
        serrno = SENORCODE ;
      if (status > 0 && rfilefdt[s_index]->offset + status < 0) {
        status = -1;
        serrno = EFBIG;
      }
      rfilefdt[s_index]->offset += status ;
      TRACE(1,"rfio","rfio_write: status %d, rcode %d",status,rcode) ;
      if ( temp ) (void) free(trp) ;
      END_TRACE() ;
      return status ;
    case RQST_READAHEAD:
    case RQST_LASTSEEK:
    case RQST_PRESEEK:
      /*
       * At this point, a temporary buffer may need to be created to
       * receive data which is going to be thrown away.
       */
      if ( temp == 0 ) {
        if ( rfilefdt[s_index]->_iobuf.base==NULL || (int)rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
          temp= 1 ;
          TRACE(2,"rfio","rfio_write: allocating momentary buffer of size %d",msgsiz) ;
          if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
            TRACE(2,"rfio","rfio_write: malloc(): ERROR occured (errno=%d)",errno) ;
            END_TRACE() ;
            return -1 ;
          }
        }
        else
          trp= iodata(rfilefdt[s_index]) ;
      }
      TRACE(2, "rfio", "rfio_write: reading %d bytes to throw them away",msgsiz) ;
      if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
        TRACE(2,"rfio", "rfio_write: read(): ERROR occured (errno=%d)", errno);
        if ( temp ) (void) free(trp) ;
        END_TRACE() ;
        return -1 ;
      }
      break ;
    default:
      TRACE(1,"rfio","rfio_write(): Bad control word received\n") ;
      serrno= SEINTERNAL ;
      if ( temp ) (void) free(trp) ;
      END_TRACE() ;
      return -1 ;
    }
  }
}
