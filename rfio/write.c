/*
 * $Id: write.c,v 1.8 2000/06/16 15:31:31 obarring Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: write.c,v $ $Revision: 1.8 $ $Date: 2000/06/16 15:31:31 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine";
#endif /* not lint */

/* write.c      Remote File I/O - write a file                          */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1  
#include "rfio.h"  

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file write
 */
int DLL_DECL rfio_write(s, ptr, size)
void    *ptr;
int     s, size;
{
   /* Remote file ? */
   if ((s >= 0) && (s < MAXRFD) && (rfilefdt[s] != NULL))
   {
      if (rfilefdt[s]->version3 == 1)
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

int rfio_write_v2(s, ptr, size) 
char    *ptr;
int     s, size;
{
   int status ;	/* Return code of called func	*/
   int HsmType, save_errno, written_to;
   char   * p ; 	/* Pointer to buffer		*/
   char * trp ; 	/* Pointer to a temp buffer	*/
   int temp=0 ;	/* Has it been allocated ? 	*/
   char     rfio_buf[BUFSIZ];

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_write(%d, %x, %d)", s, ptr, size) ;

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logwr(s,size);
#endif

   /*
    * Check HSM type and if file has been written to. The CASTOR HSM
    * uses normal RFIO (local or remote) to perform the I/O. Thus we 
    * don't call rfio_HsmIf_write().
    */
   HsmType = rfio_HsmIf_GetHsmType(s,&written_to); 
   if ( HsmType > 0 ) {
       if ( written_to == 0 ) status = rfio_HsmIf_FirstWrite(s,ptr,size);
       if ( status == -1 ) return(-1);
       if ( HsmType != RFIO_HSM_CNS ) {
           status = rfio_HsmIf_write(s,ptr,size);
           if ( status == -1 ) rfio_HsmIf_IOError(s,errno);
           return(status);
       }
   }

   /*
    * The file is local.
    */
   if ( s<0 || s>= MAXRFD || rfilefdt[s] == NULL) {
      TRACE(2, "rfio", "rfio_write: using local write(%d, %x, %d)", s, ptr, size);
      status = write(s, ptr, size);
      if ( HsmType == RFIO_HSM_CNS ) {
          save_errno = errno;
          rfio_HsmIf_IOError(s,errno);
          errno = save_errno;
      }
      END_TRACE();
      rfio_errno = 0;
      return(status);
   }

   /*
    * Checking magic number.
    */
   if (rfilefdt[s]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ; 
      (void) close(s) ;
      free((char *)rfilefdt[s]);
      rfilefdt[s] = NULL ;
      END_TRACE();
      return(-1);
   }
   /*
    * Repositionning file mark if needed.
    */
   if ( (rfilefdt[s]->readissued || rfilefdt[s]->preseek) && rfilefdt[s]->lseekhow == -1 ) {
      rfilefdt[s]->lseekhow= SEEK_SET ;
      rfilefdt[s]->lseekoff= rfilefdt[s]->offset ;
   }
   /*
    * Resetting flags
    */
   rfilefdt[s]->eof= 0 ; 
   rfilefdt[s]->preseek= 0 ;
   rfilefdt[s]->nbrecord= 0 ;
   rfilefdt[s]->readissued= 0 ; 
   if ( rfilefdt[s]->_iobuf.base ) {
      rfilefdt[s]->_iobuf.count= 0 ; 
      rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ; 
   }
   /*
    * Sending request.
    */
   p= rfio_buf ;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_WRITE);
   marshall_LONG(p, size);
   marshall_LONG(p, rfilefdt[s]->lseekhow) ; 
   marshall_LONG(p, rfilefdt[s]->lseekoff) ; 
   rfilefdt[s]->lseekhow= -1 ;
   TRACE(2, "rfio", "rfio_write: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2,"rfio","rfio_write: write(): ERROR occured (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }
   if (rfilefdt[s]->bufsize < size) {
      rfilefdt[s]->bufsize = size ;
      TRACE(2, "rfio", "rfio_write: setsockopt(SOL_SOCKET, SO_SNDBUF): %d", rfilefdt[s]->bufsize) ;
      if (setsockopt(s,SOL_SOCKET,SO_SNDBUF,(char *)&(rfilefdt[s]->bufsize),sizeof((rfilefdt[s]->bufsize))) == -1) {
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

      TRACE(2, "rfio", "rfio_write: reading %d bytes",rfilefdt[s]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s]->_iobuf.hsize,RFIO_CTRL_TIMEOUT) != rfilefdt[s]->_iobuf.hsize) {
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
	  rfilefdt[s]->offset += status ;
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
	     if ( rfilefdt[s]->_iobuf.base==NULL || rfilefdt[s]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(2,"rfio","rfio_write: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(2,"rfio","rfio_write: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return -1 ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s]) ;
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
	  rfio_errno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return -1 ;
      }
   }
}
