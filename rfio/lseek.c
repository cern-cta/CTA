/*
 * $Id: lseek.c,v 1.11 2002/01/18 16:23:29 obarring Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lseek.c,v $ $Revision: 1.11 $ $Date: 2002/01/18 16:23:29 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* lseek.c      Remote File I/O - move read/write file mark.	*/

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1  
#include "rfio.h"     
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Forward declaration.
 */
static int rfio_lseekinbuf() ; 
static int rfio_forcelseek() ;

/*
 * Remote file seek
 */
int DLL_DECL rfio_lseek(s, offset, how)   
int      s ;
int offset ; 
int    how ;
{
   int     status ;
   char     rfio_buf[BUFSIZ];
   int s_index = -1;

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_lseek(%d, %d, %x)",s,offset,how) ;

#if defined(CLIENTLOG)
   /* Client logging */
   rfio_logls(s,offset,how);
#endif /* CLIENTLOG */

   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1 ) {
      TRACE(2, "rfio", "rfio_lseek: using local lseek(%d, %d, %d)",s,offset,how) ;
      status= lseek(s,offset,how) ; 
      rfio_errno = 0;
      END_TRACE() ;
      return status ;
   }
   /*
    * Checking 'how' parameter.
    */
   if ( how < 0 || how > 2 ) {
      errno= EINVAL ;
      END_TRACE() ; 
      return -1 ; 
   }
   /*
    * Checking magic number
    */
   if ( rfilefdt[s_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ;
      rfio_rfilefdt_freeentry(s_index);
      (void) close(s);
      END_TRACE();
      return -1 ;
   }

   /* If RFIO version 3 enabled, then call the corresponding procedure */
   if (rfilefdt[s_index]->version3 == 1)
     return(rfio_lseek_v3(s,offset,how));

   /*
    * Changing SEEK_CUR in SEEK_SET.
    * WARNING !!! Should not be removed.
    */
   if ( how == SEEK_CUR ) {
      how=SEEK_SET ;
      offset+= rfilefdt[s_index]->offset ;
   }
   /*
    * A preseek() is active.
    */
   if ( rfilefdt[s_index]->preseek && how != SEEK_END ) {
      status= rfio_lseekinbuf(s,offset) ;
      END_TRACE() ;
      return status ;
   }
   /*
    * If I/O are bufferized and 
    * if the buffer is not empty.
    */
   if ( rfilefdt[s_index]->_iobuf.base && rfilefdt[s_index]->_iobuf.count ) {
      if ( how != SEEK_END ) {
	 if ( offset >= rfilefdt[s_index]->offset ) {
	    /*
	     * Data is currently in the buffer.
	     */
	    if ( (offset - rfilefdt[s_index]->offset) <= rfilefdt[s_index]->_iobuf.count ) {
	       rfilefdt[s_index]->_iobuf.count -= offset - rfilefdt[s_index]->offset ;
	       rfilefdt[s_index]->_iobuf.ptr += offset - rfilefdt[s_index]->offset ;
	       rfilefdt[s_index]->offset= offset ; 
	       END_TRACE() ;
	       return offset ; 
	    }
	    /*
	     * Data should be in the next message.
	     */
	    else
	       if (rfilefdt[s_index]->readissued && (offset-rfilefdt[s_index]->offset)<=(rfilefdt[s_index]->_iobuf.count+rfilefdt[s_index]->_iobuf.dsize)){
		  /*
		   * Getting next message.
		   */
		  rfilefdt[s_index]->offset += rfilefdt[s_index]->_iobuf.count ;
		  rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ;
		  rfilefdt[s_index]->_iobuf.count= 0 ;
		  status= rfio_filbuf(s,rfilefdt[s_index]->_iobuf.base,rfilefdt[s_index]->_iobuf.dsize) ;
		  if ( status < 0 ) {
		     rfilefdt[s_index]->readissued= 0 ; 
		     END_TRACE() ;
		     return -1 ;
		  }
		  if ( status != rfilefdt[s_index]->_iobuf.dsize  ) {
		     rfilefdt[s_index]->eof= 1 ; 
		     rfilefdt[s_index]->readissued= 0 ;
		  }
		  rfilefdt[s_index]->_iobuf.count= status ;
		  /*
		   * Setting buffer pointers to the right place if possible.
		   */
		  if ( (offset - rfilefdt[s_index]->offset) <= rfilefdt[s_index]->_iobuf.count ) {
		     rfilefdt[s_index]->_iobuf.count -= offset - rfilefdt[s_index]->offset ;
		     rfilefdt[s_index]->_iobuf.ptr += offset - rfilefdt[s_index]->offset ;
		     rfilefdt[s_index]->offset= offset ; 
		     END_TRACE() ;
		     return offset ; 
		  }	
	       }
	 }
	 else 	{
	    if ((rfilefdt[s_index]->offset-offset)<=(rfilefdt[s_index]->_iobuf.dsize-rfilefdt[s_index]->_iobuf.count) &&
		( rfilefdt[s_index]->offset - offset ) <=  ( rfilefdt[s_index]->_iobuf.ptr - rfilefdt[s_index]->_iobuf.base) ) {
	       rfilefdt[s_index]->_iobuf.count += rfilefdt[s_index]->offset - offset ;
	       rfilefdt[s_index]->_iobuf.ptr -= rfilefdt[s_index]->offset - offset ;
	       rfilefdt[s_index]->offset= offset ;
	       END_TRACE() ;
	       return offset ;
	    }
	 }
      }
   }
   rfilefdt[s_index]->lseekhow= how ;
   rfilefdt[s_index]->lseekoff= offset ;
   if ( how == SEEK_END) {
      status= rfio_forcelseek(s,offset,how) ; 
      rfilefdt[s_index]->eof= 1 ;
      rfilefdt[s_index]->offset= status ;
      rfilefdt[s_index]->lseekoff= status ;
   }
   else	{
      rfilefdt[s_index]->offset= offset ;
   }
   END_TRACE() ;
   return rfilefdt[s_index]->offset ;
}

/*
 * Action taken when a preseek() is active and rfio_lseek() is called.
 * Positionned pointers to the right record.
 */
static int rfio_lseekinbuf(s,offset)
int      s ;
int offset ; 
{
   char   * p ;	/* Pointer to buffer		*/
   int s_index;

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_lseekinbuf(%d,%d)",s,offset) ;

   /*
    * Scanning records already requested.
    */
   s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN);

   for(;;rfilefdt[s_index]->nbrecord -- ) {
      int  status ; 	/* Status of the read() request	*/
      int   rcode ;	/* Error code if any		*/
      int     off ; 	/* Offset of the current record	*/
      int	len ;	/* Length requested		*/

      /*
       * The buffer is empty.
       */
      if ( rfilefdt[s_index]->nbrecord == 0 ) {
	 WORD	req ;
	 int  msgsiz ;

	 /*
	  * No more data will arrive.
	  */
	 if ( rfilefdt[s_index]->preseek == 2 ) 
	    break ;
	 /*
	  * Filling the buffer.
	  */
	 msgsiz= rfilefdt[s_index]->_iobuf.hsize + rfilefdt[s_index]->_iobuf.dsize ;
	 TRACE(2,"rfio","rfio_lseekinbuf: reading %d bytes",msgsiz) ; 
	 if ( netread_timeout(s,rfilefdt[s_index]->_iobuf.base,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	    TRACE(2,"rfio","rfio_lseekinbuf: read() : ERROR occured (errno=%d)",errno) ;
	    break ; 
	 }
	 p= rfilefdt[s_index]->_iobuf.base ;
	 unmarshall_WORD(p,req) ; 
	 unmarshall_LONG(p,status) ; 
	 unmarshall_LONG(p,rcode) ;
	 unmarshall_LONG(p,msgsiz) ; 
	 rfio_errno= rcode ;
	 /*
	  * There is an error,
	  * it did not work correctly.
	  */
	 if ( status == -1 ) 
	    break ; 
	 /*
	  * Resetting pointers.
	  */
	 rfilefdt[s_index]->nbrecord= status ;
	 rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ; 
	 rfilefdt[s_index]->preseek= ( req == RQST_LASTSEEK ) ? 2 : 1 ;
      }
      /*
       * The current record is the one we are looking for.
       */
      p= rfilefdt[s_index]->_iobuf.ptr ;
      unmarshall_LONG(p,off) ; 
      unmarshall_LONG(p,len) ; 
      TRACE(2,"rfio","rfio_lseekinbuf: current record is at offset %d and of length %d",off,len) ; 
      if ( off <= offset && offset < off + len ) {
	 rfilefdt[s_index]->offset= offset ;
	 END_TRACE() ;
	 return offset ;
      }
      /*
       * Pointing to the next record.
       */
      unmarshall_LONG(p,status) ; 
      unmarshall_LONG(p, rcode) ; 
      if ( status > 0 ) 
	 rfilefdt[s_index]->_iobuf.ptr= p + status ;
      else
	 rfilefdt[s_index]->_iobuf.ptr= p ;
   }
   /*
    * The offset requested in the preseek 
    * does not appear in the preseek list.
    * an lseek() will be done with the next read().
    */
   rfilefdt[s_index]->nbrecord= 0 ; 
   rfilefdt[s_index]->preseek= 0 ; 
   rfilefdt[s_index]->lseekhow= SEEK_SET ;
   rfilefdt[s_index]->lseekoff= offset ;
   rfilefdt[s_index]->offset= offset ;
   END_TRACE() ; 
   return offset ;
}

/*
 * Forcing remote lseek().
 * Necessarry for SEEK_END lseek().
 */
static int rfio_forcelseek(s, offset, how)   
int      s ;
int offset ; 
int    how ;
{
   char   * p ;	/* Pointer to buffer		*/
   char * trp ;	/* Pointer to temporary buffer	*/
   int temp=0 ;	/* Is there a temporary buffer?	*/
   char     rfio_buf[BUFSIZ];
   int s_index;

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1, "rfio", "rfio_forcelseek(%d, %d, %x)", s, offset, how) ;
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
     serrno = SEINTERNAL;
     TRACE(2, "rfio", "rfio_lseek: rfio_rfilefdt_findentry(): ERROR occured (serrno=%d)", serrno);
     END_TRACE() ;
     return -1 ;
   }
   if ( rfilefdt[s_index]->ahead ) rfilefdt[s_index]->readissued= 0 ; 
   rfilefdt[s_index]->preseek= 0 ;
   rfilefdt[s_index]->nbrecord= 0 ; 
   rfilefdt[s_index]->eof= 0 ; 
   /*
    * If RFIO are buffered,
    * data in cache is invalidate.
    */
   if ( rfilefdt[s_index]->_iobuf.base ) {
      rfilefdt[s_index]->_iobuf.count=0;
      rfilefdt[s_index]->_iobuf.ptr = iodata(rfilefdt[s_index]) ;
   }
   /*
    * Sending request.
    */
   p= rfio_buf ; 
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_LSEEK);
   marshall_LONG(p, offset);
   marshall_LONG(p, how);
   TRACE(2, "rfio", "rfio_forcelseek: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_lseek: write(): ERROR occured (errno=%d)", errno);
      END_TRACE() ;
      return -1 ;
   }
   /*
    * If unbuffered I/O, a momentary buffer 
    * is created to read unnecessary data and
    * to throw it away.
    */
   if ( rfilefdt[s_index]->_iobuf.base == NULL ) {
      TRACE(3,"rfio","rfio_forcelseek: allocating momentary buffer of size %d",rfilefdt[s_index]->_iobuf.dsize) ; 
      if ( (trp= ( char *) malloc(rfilefdt[s_index]->_iobuf.dsize)) == NULL ) {
	 TRACE(3,"rfio","rfio_forcelseek: malloc(): ERROR occured (errno=%d)",errno) ; 
	 END_TRACE() ; 
	 return -1 ;
      }
   }
   else	{
      trp= iodata(rfilefdt[s_index]) ; 
   }
   /*
    * Getting data from the network.
    */
   for(;;) {
      WORD 	req ; 
      int  status ;
      int   rcode ;
      int  msgsiz ;

      TRACE(2, "rfio", "rfio_forcelseek: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s_index]->_iobuf.hsize) {
	 TRACE(2,"rfio","rfio_forcelseek: read(): ERROR occured (errno=%d)",errno) ;
	 if ( temp ) (void) free(trp) ; 
	 END_TRACE() ;
	 return -1 ; 
      }
      p = rfio_buf ;
      unmarshall_WORD(p,req) ;
      unmarshall_LONG(p,status) ;
      unmarshall_LONG(p,rcode)  ; 
      unmarshall_LONG(p,msgsiz) ; 
      switch(req) {
       case RQST_LSEEK:
	  rfio_errno = rcode ;
	  if ( temp ) (void) free(trp) ; 
	  TRACE(1,"rfio","rfio_lseek: status %d, rcode %d",status,rcode) ;
	  END_TRACE() ; 
	  return status ;
       case RQST_READAHEAD:
       case RQST_LASTSEEK:
       case RQST_PRESEEK:
	  /* 
	   * At this point a temporary buffer may need to be created
	   * to receive data which is going to be thrown away.
	   */
	  if ( temp == 0 ) {
	     if ( rfilefdt[s_index]->_iobuf.base==NULL || rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(3,"rfio","rfio_forcelseek: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(3,"rfio","rfio_forcelseek: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return -1 ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  TRACE(2,"rfio","rfio_forcelseek: reading %d bytes to throw them away",msgsiz) ; 
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio", "rfio_forcelseek: read(): ERROR occured (errno=%d)", errno);
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return -1 ;
	  }
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_forcelseek(): Bad control word received") ; 
	  rfio_errno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return -1 ;
      }
   }	
}
