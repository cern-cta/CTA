/*
 * $Id: preseek.c,v 1.7 2000/09/20 13:52:51 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: preseek.c,v $ $Revision: 1.7 $ $Date: 2000/09/20 13:52:51 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* preseek.c      Remote File I/O - preseeking.		*/

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1  
#ifdef linux
#include <sys/uio.h>
#endif
#include "rfio.h"     

#include <stdlib.h>            /* malloc prototype */

#ifndef min
#define min(a,b)	(((a)<(b)) ? (a):(b))
#endif

/*
 * Remote file seek
 */
int DLL_DECL rfio_preseek(s,iov,iovnb)   
int      s ;
int  iovnb ;
struct iovec *iov ;
{
   char   * p ;		/* Pointer to buffer		*/
   int	 i ; 		/* Loop index			*/
   int    temp = 0 ; 	/* Temporary buffer exists ?	*/
   char *trp= NULL ; 	/* Pointer to temporary buffer	*/
   char     rfio_buf[BUFSIZ];
	
   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_preseek(%d, %x, %d)",s,iov,iovnb) ;
   /*
    * The file is local.
    * Nothing has to be done.
    */
   if ( s >= MAXRFD || rfilefdt[s] == NULL ) {
      END_TRACE() ;
      return 0 ;
   }
   /*
    * Checking magic number
    */
   if ( rfilefdt[s]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ;
      free((char *)rfilefdt[s]);
      rfilefdt[s] = NULL ;
      (void) close(s);
      END_TRACE();
      return -1 ;
   }
   /*
    * Nothing specified.
    */
   if ( iovnb == 0 ) {
      END_TRACE() ;
      return 0 ; 
   }
   /*
    * Repositionning file mark if needed.
    */
   if ( (rfilefdt[s]->readissued || rfilefdt[s]->preseek) && rfilefdt[s]->lseekhow == -1 ) {
      rfilefdt[s]->lseekhow= SEEK_SET ;
      rfilefdt[s]->lseekoff= rfilefdt[s]->offset ;
   }
   /*
    * Resetting flags.
    * preseek() is forbidden when RFIO aren't buffered.
    */
   rfilefdt[s]->eof= 0 ; 
   rfilefdt[s]->preseek= 0 ;
   rfilefdt[s]->nbrecord= 0 ;
   rfilefdt[s]->readissued= 0 ; 
   if ( rfilefdt[s]->_iobuf.base ) {
      rfilefdt[s]->_iobuf.count= 0 ; 
      rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ; 
   }
   else 	{
      errno= EINVAL ; 
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Is rfio_buf large enough to contain the request ? 
    * If not a temporary buffer is allocated.
    * THe request is then sent.
    */
   if ( (BUFSIZ-RQSTSIZE)/(2*LONGSIZE) < iovnb ) {
      temp= 1 ;
      if ((trp= (char *)malloc(RQSTSIZE+2*LONGSIZE*iovnb)) == NULL) return -1 ;
   }
   else	{
      trp= rfio_buf ;
   }
   p= trp ;
   marshall_WORD(p,RFIO_MAGIC) ;
   marshall_WORD(p,RQST_PRESEEK) ;
   marshall_LONG(p,rfilefdt[s]->_iobuf.dsize) ;
   marshall_LONG(p,iovnb) ;
   p= trp + RQSTSIZE ;
   for(i= 0; i<iovnb; i ++) {
      marshall_LONG(p,iov[i].iov_base) ;
      marshall_LONG(p,iov[i].iov_len) ;
   }
   TRACE(2,"rfio","rfio_preseek: sending %d bytes",RQSTSIZE+2*LONGSIZE*iovnb) ;
   if (netwrite_timeout(s,trp,RQSTSIZE+2*LONGSIZE*iovnb,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+2*LONGSIZE*iovnb)) {
      TRACE(2,"rfio","rfio_preseek: write(): ERROR occured (errno=%d)",errno) ;
      if ( temp ) (void) free(trp) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Freeing the temporary buffer if 
    * it has been created.
    */
   if ( temp ) (void) free(trp) ;
   /*
    * Getting data from the network.
    */
   for(;;) {
      WORD 	req ; 
      int  status ;
      int   rcode ;
      int  msgsiz ;

      msgsiz= rfilefdt[s]->_iobuf.hsize + rfilefdt[s]->_iobuf.dsize ;
      TRACE(2, "rfio", "rfio_preseek: reading %d bytes",msgsiz) ; 
      if (netread_timeout(s,rfilefdt[s]->_iobuf.base,msgsiz,RFIO_CTRL_TIMEOUT) != msgsiz) {
	 TRACE(2,"rfio","rfio_preseek: read(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ; 
      }
      p= rfilefdt[s]->_iobuf.base ;
      unmarshall_WORD(p,req) ;
      unmarshall_LONG(p,status) ;
      unmarshall_LONG(p,rcode)  ; 
      unmarshall_LONG(p,msgsiz) ; 
      switch(req) {
       case RQST_FIRSTSEEK:
	  rfio_errno = rcode ;
	  TRACE(1,"rfio","rfio_preseek: status %d, rcode %d",status,rcode) ;
	  if ( status == -1 ) {
	     END_TRACE() ; 
	     return -1 ;
	  }
	  rfilefdt[s]->preseek= ( status == iovnb ) ? 2 : 1 ; 
	  rfilefdt[s]->nbrecord= status ; 
	  rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ; 
	  rfilefdt[s]->_iobuf.count= 0 ; 
	  END_TRACE() ; 
	  return 0 ;
       case RQST_PRESEEK:
       case RQST_LASTSEEK:
       case RQST_READAHEAD:
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_preseek(): Bad control word received") ; 
	  rfio_errno= SEINTERNAL ;
	  END_TRACE() ; 
	  return -1 ;
      }
   }	
}

/*
 * FORTRAN bindings.
 */
#if defined(CRAY)
#include <fortran.h>		/* FORTRAN to C conversion macros	*/
#endif


#if defined(CRAY)
int PRESEE(sptr,iov,iovnbptr) 
#else 
#if defined(_AIX)
int DLL_DECL presee(sptr,iov,iovnbptr)
#else	
int DLL_DECL presee_(sptr,iov,iovnbptr)
#endif	/* AIX	*/
#endif	/* CRAY	*/	
int 	    * sptr ;
struct iovec * iov ;
int     * iovnbptr ;
{
   int status ;

   status= rfio_preseek(*sptr,iov,*iovnbptr) ; 
   return status ;
}

