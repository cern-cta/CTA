/*
 * $Id: write64.c,v 1.1 2002/11/19 10:51:23 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: write64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:23 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine, Philippe Gaillardon";
#endif /* not lint */

/* write64.c      Remote File I/O - write a file                          */

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
int DLL_DECL rfio_write64(s, ptr, size)
void    *ptr;
int     s, size;
{
  int s_index;

   /* Remote file ? */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) != -1)
   {
      if (rfilefdt[s_index]->version3 == 1)
      {
	 /* New V3 stream protocol for sequential transfers */
	 return(rfio_write64_v3(s,(char *)ptr,size));
      }
      else
	 return(rfio_write64_v2(s,(char *)ptr,size));
   }
   else
      return(rfio_write64_v2(s,(char *)ptr,size));
}

int rfio_write64_v2(s, ptr, size) 
char    *ptr;
int     s, size;
{
   int status ;	/* Return code of called func	*/
   int HsmType, save_errno, written_to;
   char   * p ; 	/* Pointer to buffer		*/
   char * trp ; 	/* Pointer to a temp buffer	*/
   int temp=0 ; 	/* Has it been allocated ?	*/
   char     rfio_buf[BUFSIZ];
   int s_index;
   RFILE *rfptr; 	/* RFILE pointer		*/

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_write64(%d, %x, %d)", s, ptr, size) ;
   if (size == 0) {
      END_TRACE();
      return(0);
   }

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
       if ( written_to == 0 && (status = rfio_HsmIf_FirstWrite(s,ptr,size)) < 0) {
           END_TRACE();
           return(status);
       }
       if ( HsmType != RFIO_HSM_CNS ) {
           status = rfio_HsmIf_write(s,ptr,size);
           if ( status == -1 ) rfio_HsmIf_IOError(s,errno);
           END_TRACE();
           return(status);
       }
   }

   /*
    * The file is local.
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_write: using local write(%d, %x, %d)", s, ptr, size);
      status = write(s, ptr, size);
      if ( status < 0 ) serrno = 0;
      if ( HsmType == RFIO_HSM_CNS ) {
          save_errno = errno;
          rfio_HsmIf_IOError(s,errno);
          errno = save_errno;
      }
      END_TRACE();
      rfio_errno = 0;
      return(status);
   }
   /* least is beautiful !                              */
   rfptr = rfilefdt[s_index];

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
   if (!rfilefdt[s_index]->mode64) {
      status = rfio_write_v2(s, ptr, size);
      END_TRACE();
      return(status);
   }

   /*
    * Repositionning file mark if needed.
    */
   if (  (rfilefdt[s_index]->lseekhow == -1)
      && ( rfilefdt[s_index]->readissued || rfilefdt[s_index]->preseek ) ) {
      rfilefdt[s_index]->lseekhow= SEEK_SET ;
      rfilefdt[s_index]->lseekoff64= rfilefdt[s_index]->offset64 ;
   }
   /*
    * Repositionning file mark if buffered read occured.
    */
   if (  rfptr->_iobuf.base && rfptr->_iobuf.count && (rfptr->lseekhow == -1)
      && (rfptr->lseekoff64 + rfptr->_iobuf.count != rfptr->offset64) ) {
      rfptr->lseekhow= SEEK_SET ;
      rfptr->lseekoff64= rfptr->offset64 ;
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
   marshall_WORD(p, RQST_WRITE64);
   marshall_LONG(p, size);
   marshall_LONG(p, rfilefdt[s_index]->lseekhow) ; 
   p= rfio_buf + RQSTSIZE ;
   marshall_HYPER(p, rfilefdt[s_index]->lseekoff64) ; 
   rfilefdt[s_index]->lseekhow= -1 ;
   TRACE(2, "rfio", "rfio_write64: sending %d bytes",RQSTSIZE64) ;
   if (netwrite_timeout(s, rfio_buf, RQSTSIZE64, RFIO_CTRL_TIMEOUT) != RQSTSIZE64) {
      TRACE(2,"rfio","rfio_write64: write(): ERROR occured (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }
   if (rfilefdt[s_index]->bufsize < size) {
      rfilefdt[s_index]->bufsize = size ;
      TRACE(2, "rfio", "rfio_write64: setsockopt(SOL_SOCKET, SO_SNDBUF): %d", rfilefdt[s_index]->bufsize) ;
      if (setsockopt(s,SOL_SOCKET,SO_SNDBUF,(char *)&(rfilefdt[s_index]->bufsize),sizeof((rfilefdt[s_index]->bufsize))) == -1) {
	 TRACE(2, "rfio" ,"rfio_write64: setsockopt(SO_SNDBUF)") ;
      }
   }
   TRACE(2,"rfio","rfio_write64: sending %d bytes",size) ;
   if (netwrite_timeout(s, ptr, size, RFIO_DATA_TIMEOUT) != size) {
      TRACE(2,"rfio","rfio_write64: write(): ERROR occured (errno=%d)",errno) ;
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

      TRACE(2, "rfio", "rfio_write64: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s_index]->_iobuf.hsize) {
	 TRACE(2, "rfio", "rfio_write64: read(): ERROR occured (errno=%d)", errno);
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
       case RQST_WRITE64:
	  rfio_errno = rcode;
          if ( status < 0 ) rfio_HsmIf_IOError(s,rfio_errno);
	  if ( status < 0 && rcode == 0 )
	     serrno = SENORCODE ;
	  rfilefdt[s_index]->offset64 += status ;
	  TRACE(1,"rfio","rfio_write64: status %d, rcode %d",status,rcode) ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ;
	  return status ;
       case RQST_READAHD64:
       case RQST_LASTSEEK:
       case RQST_PRESEEK64:
	  /*
	   * At this point, a temporary buffer may need to be created to
	   * receive data which is going to be thrown away.
	   */
	  if ( temp == 0 ) {
	     if ( rfilefdt[s_index]->_iobuf.base==NULL || rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(2,"rfio","rfio_write64: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(2,"rfio","rfio_write64: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return -1 ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  TRACE(2, "rfio", "rfio_write64: reading %d bytes to throw them away",msgsiz) ; 
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio", "rfio_write64: read(): ERROR occured (errno=%d)", errno);
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return -1 ;
	  }
	  break ; 
       default:	
	  TRACE(1,"rfio","rfio_write64(): Bad control word received\n") ; 
	  serrno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return -1 ;
      }
   }
}
