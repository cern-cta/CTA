/*
 * $Id: lseek64.c,v 1.1 2002/11/19 10:51:23 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lseek64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:23 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, P. Gaillardon";
#endif /* not lint */

/* lseek64.c      Remote File I/O - move read/write file mark.	*/

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1  
#include "rfio.h"     
#include "rfio_rfilefdt.h"
#include "u64subr.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Forward declaration.
 */
static off64_t rfio_lseekinbuf64(int s, off64_t offset) ; 
static off64_t rfio_forcelseek64(int s, off64_t offset, int how) ;

/*
 * Remote file seek
 */
off64_t DLL_DECL rfio_lseek64(s, offset, how)   
int      s ;
off64_t  offset ; 
int      how ;
{
   int      status ;
   off64_t  offsetout;
   char     rfio_buf[BUFSIZ];
   int      s_index = -1;
   char     tmpbuf[21];

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_lseek64(%d, %s, %x)",s,i64tostr(offset,tmpbuf,0),how) ;

#if defined(CLIENTLOG)
   /* Client logging */
   rfio_logls(s,offset,how);
#endif /* CLIENTLOG */

   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1 ) {
      TRACE(2, "rfio", "rfio_lseek64: using local lseek64(%d, %s, %d)",s,i64tostr(offset,tmpbuf,0),how) ;
      offsetout = lseek64(s,offset,how) ; 
      if ( offsetout < 0 ) serrno = 0;
      END_TRACE();
      rfio_errno = 0;
      return offsetout ;
   }
   /*
    * Checking 'how' parameter.
    */
   if ( how < 0 || how > 2 ) {
      errno= EINVAL ;
      END_TRACE();
      return -1 ; 
   }
   /*
    * Checking 'offset' parameter.
    */
   if ( offset < 0 && how == SEEK_SET ) {
#if (defined(__osf__) && defined(__alpha)) || defined(hpux) || defined(_WIN32)
      errno= EINVAL ;
#else
      errno= EOVERFLOW ;
#endif
      END_TRACE();
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

   /*
    * Checking mode 64.
    */
   if (!rfilefdt[s_index]->mode64) {
      int off32in;
      int off32out;
      
      if (offset > (off64_t)2147483647 ) {
         errno = EFBIG;
         END_TRACE();
         return ((off64_t)-1);
      }
      off32in = offset;
      off32out = rfio_lseek(s, off32in, how);
      END_TRACE();
      return((off64_t)off32out);
   }

   /* If RFIO version 3 enabled, then call the corresponding procedure */
   if (rfilefdt[s_index]->version3 == 1) {
     offsetout = rfio_lseek64_v3(s,offset,how);
     END_TRACE();
     return(offsetout);
   }

   /*
    * Changing SEEK_CUR in SEEK_SET.
    * WARNING !!! Should not be removed.
    */
   if ( how == SEEK_CUR ) {
      how = SEEK_SET ;
      offset += rfilefdt[s_index]->offset64 ;
   }
   /*
    * A preseek() is active.
    */
   if ( rfilefdt[s_index]->preseek && how != SEEK_END ) {
      offsetout = rfio_lseekinbuf64(s,offset) ;
      END_TRACE() ;
      return offsetout ;
   }
   /*
    * If I/O are bufferized and 
    * if the buffer is not empty.
    */
   if ( rfilefdt[s_index]->_iobuf.base && rfilefdt[s_index]->_iobuf.count ) {
      if ( how != SEEK_END ) {
	 if ( offset >= rfilefdt[s_index]->offset64 ) {
	    if ( offset <= rfilefdt[s_index]->offset64 + rfilefdt[s_index]->_iobuf.count ) {
	       /*
	        * Data is currently in the buffer.
	        */
	       rfilefdt[s_index]->_iobuf.count -= (offset - rfilefdt[s_index]->offset64) ;
	       rfilefdt[s_index]->_iobuf.ptr   += (offset - rfilefdt[s_index]->offset64) ;
	       rfilefdt[s_index]->offset64      = offset ; 
	       END_TRACE() ;
	       return offset ; 
	    }
	    /*
	     * Data should be in the next message.
	     */
	    else
	       if (rfilefdt[s_index]->readissued &&
                  (offset-rfilefdt[s_index]->offset64)<=(rfilefdt[s_index]->_iobuf.count+rfilefdt[s_index]->_iobuf.dsize)){
		  /*
		   * Getting next message.
		   */
		  rfilefdt[s_index]->offset64 += rfilefdt[s_index]->_iobuf.count ;
		  rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ;
		  rfilefdt[s_index]->_iobuf.count= 0 ;
		  status= rfio_filbuf64(s,rfilefdt[s_index]->_iobuf.base,rfilefdt[s_index]->_iobuf.dsize) ;
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
		  if ( (offset - rfilefdt[s_index]->offset64) <= rfilefdt[s_index]->_iobuf.count ) {
		     rfilefdt[s_index]->_iobuf.count -= (offset - rfilefdt[s_index]->offset64) ;
		     rfilefdt[s_index]->_iobuf.ptr   += (offset - rfilefdt[s_index]->offset64) ;
		     rfilefdt[s_index]->offset64      = offset ; 
		     END_TRACE() ;
		     return offset ; 
		  }	
	       }
	 }
	 else 	{
	    if ((rfilefdt[s_index]->offset64-offset)<=(rfilefdt[s_index]->_iobuf.dsize-rfilefdt[s_index]->_iobuf.count) &&
		(rfilefdt[s_index]->offset64-offset)<=(rfilefdt[s_index]->_iobuf.ptr - rfilefdt[s_index]->_iobuf.base) ) {
	       rfilefdt[s_index]->_iobuf.count += (rfilefdt[s_index]->offset64 - offset) ;
	       rfilefdt[s_index]->_iobuf.ptr   -= (rfilefdt[s_index]->offset64 - offset) ;
	       rfilefdt[s_index]->offset64      = offset ;
	       END_TRACE() ;
	       return offset ;
	    }
	 }
      }  /* how != SEEK_END */
   }  /* Non empty buffer */
   rfilefdt[s_index]->lseekhow  = how ;
   rfilefdt[s_index]->lseekoff64= offset ;
   
   if ( how == SEEK_END) {
      offsetout = rfio_forcelseek64(s,offset,how) ; 
      rfilefdt[s_index]->eof = 1 ;
      rfilefdt[s_index]->offset64 = offsetout ;
      rfilefdt[s_index]->lseekhow = -1 ;
      rfilefdt[s_index]->lseekoff64 = offsetout ;
   }
   else	{
      rfilefdt[s_index]->offset64 = offset ;
   }
   END_TRACE() ;
   return rfilefdt[s_index]->offset64 ;
}

/*
 * Action taken when a preseek() is active and rfio_lseek() is called.
 * Positionned pointers to the right record.
 */
static off64_t rfio_lseekinbuf64(s,offset)
int      s ;
off64_t  offset ; 
{
   char   * p ;	/* Pointer to buffer		*/
   int s_index;
   char     tmpbuf[21];

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_lseekinbuf64(%d,%s)",s,i64tostr(offset,tmpbuf,0)) ;

   /*
    * Scanning records already requested.
    */
   s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN);

   for(;;rfilefdt[s_index]->nbrecord -- ) {
      int      status ;    /* Status of the read() request      */
      int      rcode ;     /* Error code if any                 */
      off64_t  curoff ;    /* Offset of the current record      */
      int      len ;       /* Length requested                  */

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
	 TRACE(2,"rfio","rfio_lseekinbuf64: reading %d bytes",msgsiz) ; 
	 if ( netread_timeout(s,rfilefdt[s_index]->_iobuf.base,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	    TRACE(2,"rfio","rfio_lseekinbuf64: read() : ERROR occured (errno=%d)",errno) ;
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
	 rfilefdt[s_index]->preseek= (req == RQST_LASTSEEK) ? 2 : 1 ;
      }
      /*
       * The current record is the one we are looking for.
       */
      p= rfilefdt[s_index]->_iobuf.ptr ;
      unmarshall_HYPER(p,curoff) ; 
      unmarshall_LONG(p,len) ; 
      TRACE(2,"rfio","rfio_lseekinbuf64: current record is at offset %s and of length %d",
         u64tostr(curoff,tmpbuf,0),len) ; 
      if ( curoff <= offset && offset < curoff + len ) {
	 rfilefdt[s_index]->offset64= offset ;
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
   rfilefdt[s_index]->lseekoff64= offset ;
   rfilefdt[s_index]->offset64= offset ;
   END_TRACE() ; 
   return offset ;
}

/*
 * Forcing remote lseek().
 * Necessarry for SEEK_END lseek().
 */
static off64_t rfio_forcelseek64(s, offset, how)   
int      s ;
off64_t  offset ; 
int      how ;
{
   char     * p ;             /* Pointer to buffer            */
   char     * trp ;           /* Pointer to temporary buffer  */
   int      temp=0 ;          /* Is there a temporary buffer? */
   char     rfio_buf[BUFSIZ];
   int      s_index;
   WORD     req ; 
   int      status ;
   int      rcode ;
   int      msgsiz ;          /* dummy long word              */
   int      rlen;             /* reply buffer length          */
   off64_t  offsetout;
   char     tmpbuf[21];

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1, "rfio", "rfio_forcelseek64(%d, %s, %x)", s, i64tostr(offset,tmpbuf,0), how) ;
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
     serrno = SEINTERNAL;
     TRACE(2, "rfio", "rfio_lseek64: rfio_rfilefdt_findentry(): ERROR occured (serrno=%d)", serrno);
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
   marshall_WORD(p, RQST_LSEEK64);
   marshall_HYPER(p, offset);
   marshall_LONG(p, how);
   TRACE(2, "rfio", "rfio_forcelseek64: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_lseek64: write(): ERROR occured (errno=%d)", errno);
      END_TRACE() ;
      return ((off64_t)-1) ;
   }

   /*
    * Getting data from the network.
    */

   rlen = rfilefdt[s_index]->_iobuf.hsize;
   for(;;) {
      TRACE(2, "rfio", "rfio_forcelseek64: reading %d bytes", rlen) ; 
      if (netread_timeout(s, rfio_buf, rlen, RFIO_DATA_TIMEOUT) != rlen) {
         TRACE(2,"rfio","rfio_forcelseek64: read(): ERROR occured (errno=%d)",errno) ;
         if ( temp ) (void) free(trp) ;
         END_TRACE() ;
         return ((off64_t)-1) ; 
      }
      p = rfio_buf ;
      unmarshall_WORD(p,req) ;
      switch(req) {
       case RQST_LSEEK64:
          unmarshall_HYPER(p, offsetout) ; 
          unmarshall_LONG(p,rcode) ;
          rfio_errno = rcode ;
          if ( temp ) (void) free(trp) ;
          TRACE(1,"rfio","rfio_lseek64: offsetout %s, rcode %d",
            u64tostr(offsetout,tmpbuf,0),rcode) ;
          END_TRACE() ; 
          return offsetout ;
       case RQST_READAHD64:
       case RQST_LASTSEEK:
       case RQST_PRESEEK64:
          unmarshall_LONG(p,status) ;
          unmarshall_LONG(p,rcode) ;
          unmarshall_LONG(p,msgsiz) ;
	  /* 
	   * At this point a temporary buffer may need to be created
	   * to receive data which is going to be thrown away.
	   */
	  if ( temp == 0 ) {
	     if ( rfilefdt[s_index]->_iobuf.base==NULL || rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(3,"rfio","rfio_forcelseek64: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(3,"rfio","rfio_forcelseek64: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return -1 ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  TRACE(2,"rfio","rfio_forcelseek64: reading %d bytes to throw them away",msgsiz) ; 
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio", "rfio_forcelseek64: read(): ERROR occured (errno=%d)", errno);
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return -1 ;
	  }
	  break ; 
       default:
          TRACE(1,"rfio","rfio_forcelseek64(): Bad control word received") ; 
          serrno= SEINTERNAL ;
          if ( temp ) (void) free(trp) ;
          END_TRACE() ; 
          return ((off64_t)-1) ;
      }
   }	
}
